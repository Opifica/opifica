package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/agent"
)

// checkoutDir is where the agent checks out the repository, relative to its
// work directory. Step working directories are relative to it.
const checkoutDir = "src"

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	Pool    *Pool
	Mirrors *Mirrors
	// Send delivers a message to the controller. It should keep trying
	// across reconnects; see NewOutbox.
	Send func(msg *opificav1.ConnectRequest, onSent func())
	// GuestMirrorURL turns the mirror's path on the host into the URL the
	// agent in the VM clones from. Hypervisor drivers expose the mirror to
	// the guest (e.g. over vsock); the default, the host path itself, only
	// works when the agent runs on the host (development).
	GuestMirrorURL func(vm VM, hostPath string) (string, error)
	// SecretTimeout bounds waiting for the controller to answer a secret
	// request. Defaults to 30s.
	SecretTimeout time.Duration
	Logger        *slog.Logger
}

// Runner runs the jobs the controller assigns, each in a fresh VM.
type Runner struct {
	cfg RunnerConfig
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	jobs     map[string]*runningJob
	secrets  map[string]chan *opificav1.SecretResponse
	draining bool
}

type runningJob struct {
	spec   *opificav1.Job
	cancel context.CancelCauseFunc
	seq    atomic.Uint64
}

var errCancelledByController = errors.New("cancelled by the controller")

// NewRunner creates a runner. Close it to stop every job.
func NewRunner(cfg RunnerConfig) *Runner {
	if cfg.GuestMirrorURL == nil {
		cfg.GuestMirrorURL = func(_ VM, p string) (string, error) { return p, nil }
	}
	if cfg.SecretTimeout <= 0 {
		cfg.SecretTimeout = 30 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{
		cfg: cfg, log: log, ctx: ctx, cancel: cancel,
		jobs: map[string]*runningJob{}, secrets: map[string]chan *opificav1.SecretResponse{},
	}
}

// Running returns the IDs of jobs this worker holds, for heartbeats. A job
// stays here until its final result has been delivered, so its lease is
// renewed until then.
func (r *Runner) Running() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(maps.Keys(r.jobs))
}

// Close cancels every job and waits for them to finish.
func (r *Runner) Close() {
	r.cancel()
	r.wg.Wait()
}

// HandleMessage handles a message from the controller.
func (r *Runner) HandleMessage(_ context.Context, msg *opificav1.ConnectResponse) {
	switch m := msg.GetMessage().(type) {
	case *opificav1.ConnectResponse_AssignJob:
		r.start(m.AssignJob.GetJob())
	case *opificav1.ConnectResponse_CancelJob:
		r.mu.Lock()
		j := r.jobs[m.CancelJob.GetJobId()]
		r.mu.Unlock()
		if j != nil {
			r.log.Info("job cancelled by the controller", "job", m.CancelJob.GetJobId(), "reason", m.CancelJob.GetReason())
			j.cancel(errCancelledByController)
		}
	case *opificav1.ConnectResponse_SecretResponse:
		r.mu.Lock()
		ch := r.secrets[m.SecretResponse.GetRequestId()]
		delete(r.secrets, m.SecretResponse.GetRequestId())
		r.mu.Unlock()
		if ch != nil {
			ch <- m.SecretResponse
		}
	case *opificav1.ConnectResponse_Drain:
		r.mu.Lock()
		r.draining = true
		r.mu.Unlock()
		r.log.Info("draining: finishing running jobs, accepting no new ones", "reason", m.Drain.GetReason())
	}
}

func (r *Runner) start(spec *opificav1.Job) {
	id := spec.GetId()
	r.mu.Lock()
	if _, dup := r.jobs[id]; dup || r.draining || r.ctx.Err() != nil {
		r.mu.Unlock()
		if !dup {
			// Not running it: let the controller give it to someone else.
			r.cfg.Send(update(id, opificav1.JobState_JOB_STATE_ERRORED, nil, "worker is shutting down or draining"), nil)
		}
		return
	}
	ctx, cancel := context.WithCancelCause(r.ctx)
	j := &runningJob{spec: spec, cancel: cancel}
	r.jobs[id] = j
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel(nil)
		state, exit, msg := r.run(ctx, j)
		r.log.Info("job finished", "job", id, "state", state, "exit", exit, "error", msg)
		var exitPtr *int32
		if state == opificav1.JobState_JOB_STATE_SUCCEEDED || state == opificav1.JobState_JOB_STATE_FAILED {
			exitPtr = &exit
		}
		// Forget the job only once the controller has its result.
		r.cfg.Send(update(id, state, exitPtr, msg), func() {
			r.mu.Lock()
			delete(r.jobs, id)
			r.mu.Unlock()
		})
	}()
}

func update(job string, state opificav1.JobState, exit *int32, msg string) *opificav1.ConnectRequest {
	return &opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_JobUpdate{JobUpdate: &opificav1.JobUpdate{
		JobId: job, State: state, ExitCode: exit, Error: msg,
	}}}
}

// syslog writes an Opifica line into the job's log.
func (r *Runner) syslog(j *runningJob, step uint32, format string, args ...any) {
	r.logChunk(j, step, opificav1.LogStream_LOG_STREAM_SYSTEM, []byte("==> "+fmt.Sprintf(format, args...)+"\n"))
}

func (r *Runner) logChunk(j *runningJob, step uint32, stream opificav1.LogStream, data []byte) {
	r.cfg.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_LogChunk{LogChunk: &opificav1.LogChunk{
		JobId: j.spec.GetId(), StepIndex: step, Sequence: j.seq.Add(1), Stream: stream, Data: data,
	}}}, nil)
}

// jobError is a failure with the state it should be reported as.
type jobError struct {
	state opificav1.JobState
	msg   string
}

func failed(format string, args ...any) *jobError {
	return &jobError{opificav1.JobState_JOB_STATE_FAILED, fmt.Sprintf(format, args...)}
}

func errored(format string, args ...any) *jobError {
	return &jobError{opificav1.JobState_JOB_STATE_ERRORED, fmt.Sprintf(format, args...)}
}

// run executes a job and returns how it ended. Errors in the infrastructure
// (VM, agent, mirror) are reported as ERRORED so the controller retries the
// job elsewhere; problems with the job itself are FAILED.
func (r *Runner) run(ctx context.Context, j *runningJob) (opificav1.JobState, int32, string) {
	spec := j.spec
	if t := spec.GetTimeout().AsDuration(); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, t, fmt.Errorf("job timed out after %v", t))
		defer cancel()
	}
	exit, jerr := r.execute(ctx, j)
	switch {
	case errors.Is(context.Cause(ctx), errCancelledByController):
		return opificav1.JobState_JOB_STATE_CANCELLED, 0, "cancelled"
	case r.ctx.Err() != nil:
		return opificav1.JobState_JOB_STATE_ERRORED, 0, "worker shut down during the job"
	case ctx.Err() != nil:
		msg := context.Cause(ctx).Error()
		r.syslog(j, 0, "%s", msg)
		return opificav1.JobState_JOB_STATE_FAILED, exit, msg
	case jerr != nil:
		r.syslog(j, 0, "%s", jerr.msg)
		return jerr.state, exit, jerr.msg
	case exit != 0:
		return opificav1.JobState_JOB_STATE_FAILED, exit, ""
	}
	return opificav1.JobState_JOB_STATE_SUCCEEDED, 0, ""
}

func (r *Runner) execute(ctx context.Context, j *runningJob) (int32, *jobError) {
	spec := j.spec
	src := spec.GetSource()
	r.cfg.Send(update(spec.GetId(), opificav1.JobState_JOB_STATE_PREPARING, nil, ""), nil)

	r.syslog(j, 0, "Fetching %s at %s", src.GetCloneUrl(), short(src.GetCommitSha()))
	mirror, err := r.cfg.Mirrors.Ensure(ctx, src.GetCloneUrl(), src.GetCredential(), src.GetCommitSha())
	switch {
	case errors.Is(err, ErrCommitNotFound):
		return 0, failed("%v", err)
	case err != nil:
		return 0, errored("fetching the repository: %v", err)
	}

	r.syslog(j, 0, "Starting a fresh VM (image %s)", spec.GetImage())
	lease, err := r.cfg.Pool.Acquire(ctx, spec.GetImage())
	if err != nil {
		return 0, errored("starting a VM: %v", err)
	}
	defer func() {
		if err := lease.Release(context.WithoutCancel(ctx)); err != nil {
			r.log.Error("destroying job VM", "job", spec.GetId(), "err", err)
		}
	}()

	client := agent.NewClient(func(ctx context.Context) (net.Conn, error) { return lease.VM.DialAgent(ctx) })
	if _, err := client.Ready(ctx, connect.NewRequest(&opificav1.ReadyRequest{})); err != nil {
		return 0, errored("the VM's agent is not responding: %v", err)
	}
	guestURL, err := r.cfg.GuestMirrorURL(lease.VM, mirror)
	if err != nil {
		return 0, errored("exposing the repository to the VM: %v", err)
	}
	if _, err := client.Checkout(ctx, connect.NewRequest(&opificav1.CheckoutRequest{
		MirrorUrl: guestURL, CommitSha: src.GetCommitSha(), Directory: checkoutDir,
	})); err != nil {
		return 0, errored("checking out %s: %v", short(src.GetCommitSha()), err)
	}

	r.cfg.Send(update(spec.GetId(), opificav1.JobState_JOB_STATE_RUNNING, nil, ""), nil)
	steps := spec.GetSteps()
	var failedExit int32
	for i, step := range steps {
		idx := uint32(i)
		name := stepName(step)
		r.syslog(j, idx, "Step %d/%d: %s", i+1, len(steps), name)
		exit, jerr := r.step(ctx, j, client, idx, step)
		if jerr != nil {
			return exit, jerr
		}
		if ctx.Err() != nil {
			return exit, nil
		}
		if exit != 0 {
			if step.GetContinueOnError() {
				r.syslog(j, idx, "Step failed with exit code %d; continuing (continue-on-error)", exit)
				continue
			}
			r.syslog(j, idx, "Step failed with exit code %d", exit)
			failedExit = exit
			break
		}
	}
	return failedExit, nil
}

func (r *Runner) step(ctx context.Context, j *runningJob, client agentClient, idx uint32, step *opificav1.Step) (int32, *jobError) {
	switch a := step.GetAction().(type) {
	case *opificav1.Step_Run:
		return r.runStep(ctx, j, client, idx, step, a.Run)
	case *opificav1.Step_Report:
		r.syslog(j, idx, "Test reports are not stored yet; skipping")
	case *opificav1.Step_Upload:
		r.syslog(j, idx, "Artifact uploads are not stored yet; skipping")
	case *opificav1.Step_Sign:
		r.syslog(j, idx, "The signing service is not available yet; skipping")
	default:
		return 0, failed("step %d has no action", idx+1)
	}
	return 0, nil
}

type agentClient interface {
	Exec(context.Context, *connect.Request[opificav1.ExecRequest]) (*connect.ServerStreamForClient[opificav1.ExecResponse], error)
}

func (r *Runner) runStep(ctx context.Context, j *runningJob, client agentClient, idx uint32, step *opificav1.Step, run *opificav1.RunAction) (int32, *jobError) {
	spec := j.spec
	env := map[string]string{
		"CI": "true", "OPIFICA": "true",
		"OPIFICA_JOB_ID": spec.GetId(), "OPIFICA_RUN_ID": spec.GetRunId(),
		"OPIFICA_COMMIT_SHA": spec.GetSource().GetCommitSha(), "OPIFICA_REF": spec.GetSource().GetRef(),
	}
	maps.Copy(env, spec.GetEnv())
	maps.Copy(env, step.GetEnv())

	var masks []string
	for _, name := range step.GetSecrets() {
		value, denied, err := r.secret(ctx, spec, name)
		if err != nil {
			return 0, errored("requesting secret %s: %v", name, err)
		}
		if denied != "" {
			return 0, failed("secret %s was denied: %s", name, denied)
		}
		env[name] = value
		masks = append(masks, value)
	}

	timeout := step.GetTimeout().AsDuration()
	if dl, ok := ctx.Deadline(); ok && (timeout == 0 || time.Until(dl) < timeout) {
		timeout = time.Until(dl)
	}
	wd := checkoutDir
	if d := run.GetWorkingDirectory(); d != "" {
		wd = checkoutDir + "/" + strings.TrimPrefix(d, "./")
	}
	stream, err := client.Exec(ctx, connect.NewRequest(&opificav1.ExecRequest{
		Command: run.GetCommand(), Shell: run.GetShell(), WorkingDirectory: wd,
		Env: env, Mask: masks, TimeoutSeconds: uint32(math.Ceil(timeout.Seconds())),
	}))
	if err != nil {
		return 0, errored("starting the step: %v", err)
	}
	defer stream.Close()
	exit := int32(-1)
	for stream.Receive() {
		switch e := stream.Msg().GetEvent().(type) {
		case *opificav1.ExecResponse_Stdout:
			r.logChunk(j, idx, opificav1.LogStream_LOG_STREAM_STDOUT, e.Stdout)
		case *opificav1.ExecResponse_Stderr:
			r.logChunk(j, idx, opificav1.LogStream_LOG_STREAM_STDERR, e.Stderr)
		case *opificav1.ExecResponse_ExitCode:
			exit = e.ExitCode
		}
	}
	if ctx.Err() != nil {
		return 0, nil
	}
	if err := stream.Err(); err != nil {
		var cerr *connect.Error
		if errors.As(err, &cerr) && cerr.Code() == connect.CodeInvalidArgument {
			return 0, failed("step %d: %s", idx+1, cerr.Message())
		}
		return 0, errored("running the step: %v", err)
	}
	if exit < 0 {
		return 0, errored("the agent did not report an exit code")
	}
	return exit, nil
}

// secret asks the controller for a secret on the job's behalf.
func (r *Runner) secret(ctx context.Context, spec *opificav1.Job, name string) (value, denied string, err error) {
	reqID := uuid.NewString()
	ch := make(chan *opificav1.SecretResponse, 1)
	r.mu.Lock()
	r.secrets[reqID] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.secrets, reqID)
		r.mu.Unlock()
	}()

	r.cfg.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_SecretRequest{SecretRequest: &opificav1.SecretRequest{
		RequestId: reqID, JobId: spec.GetId(), JobToken: spec.GetJobToken(), Name: name,
	}}}, nil)
	select {
	case resp := <-ch:
		if d, ok := resp.GetResult().(*opificav1.SecretResponse_DeniedReason); ok {
			return "", d.DeniedReason, nil
		}
		return string(resp.GetValue()), "", nil
	case <-time.After(r.cfg.SecretTimeout):
		return "", "", errors.New("no answer from the controller")
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
}

func stepName(s *opificav1.Step) string {
	if s.GetName() != "" {
		return s.GetName()
	}
	switch a := s.GetAction().(type) {
	case *opificav1.Step_Run:
		line, _, _ := strings.Cut(strings.TrimSpace(a.Run.GetCommand()), "\n")
		return line
	case *opificav1.Step_Report:
		return "report " + strings.Join(a.Report.GetPaths(), " ")
	case *opificav1.Step_Upload:
		return "upload " + strings.Join(a.Upload.GetPaths(), " ")
	case *opificav1.Step_Sign:
		return "sign " + strings.Join(a.Sign.GetPaths(), " ")
	}
	return "step"
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
