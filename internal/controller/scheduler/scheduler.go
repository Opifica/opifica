// Package scheduler turns pipelines into runs and jobs, hands queued jobs to
// matching workers, and follows each job to its end.
//
// All state lives in PostgreSQL. Claiming a job is one statement using
// SELECT ... FOR UPDATE SKIP LOCKED, so several controllers can schedule at
// once without giving a job to two workers.
//
// A worker holds each job it was given under a lease, renewed by its
// heartbeats. If the lease expires (the worker crashed or lost its network),
// the job is queued again, up to its maximum number of attempts.
package scheduler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/pipeline"
)

// Defaults for Config.
const (
	DefaultLease    = 60 * time.Second
	DefaultInterval = 2 * time.Second
)

// Config tunes the scheduler. Zero values use the defaults.
type Config struct {
	// Lease is how long a worker holds a job without renewing it. Workers
	// renew with every heartbeat (every 10s by default).
	Lease time.Duration
	// Interval is how often the scheduler looks for work and expired leases
	// even when nothing woke it.
	Interval time.Duration
}

// Scheduler implements workers.MessageHandler.
type Scheduler struct {
	store    *store.Store
	sessions *workers.Registry
	log      *slog.Logger
	cfg      Config
	kick     chan struct{}
}

var _ workers.MessageHandler = (*Scheduler)(nil)

// New creates a scheduler. Call Run to start it.
func New(st *store.Store, sessions *workers.Registry, log *slog.Logger, cfg Config) *Scheduler {
	if cfg.Lease <= 0 {
		cfg.Lease = DefaultLease
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: st, sessions: sessions, log: log, cfg: cfg, kick: make(chan struct{}, 1)}
}

// Kick makes the scheduler look for work now.
func (s *Scheduler) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Run schedules until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		s.reapLeases(ctx)
		s.propagateCancels(ctx)
		s.assign(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		case <-s.kick:
		}
	}
}

// ---------------------------------------------------------------- runs

// RunRequest asks for a new run.
type RunRequest struct {
	Repo     string
	CloneURL string
	Event    pipeline.Event
	// Pool comes from the controller's routing policy, never from the
	// pipeline file.
	Pool string
	// Pipeline is the pipeline file as it is at Event.SHA.
	Pipeline []byte
}

// ErrNotTriggered means the event doesn't match the pipeline's triggers.
var ErrNotTriggered = errors.New("the event does not trigger this pipeline")

// ErrNothingToRun means every job was skipped by its condition.
var ErrNothingToRun = errors.New("no job's condition matched; nothing to run")

// CreateRun plans the pipeline for the event and stores the run and its jobs.
func (s *Scheduler) CreateRun(ctx context.Context, req RunRequest) (db.Run, error) {
	if req.Repo == "" || req.Pool == "" || req.Event.SHA == "" {
		return db.Run{}, errors.New("repo, pool and commit SHA are required")
	}
	p, err := pipeline.Parse(req.Pipeline)
	if err != nil {
		return db.Run{}, err
	}
	if !p.Matches(req.Event) {
		return db.Run{}, ErrNotTriggered
	}
	plan, err := p.Plan(req.Event)
	if err != nil {
		return db.Run{}, err
	}
	if len(plan.Jobs) == 0 {
		return db.Run{}, ErrNothingToRun
	}

	runID := uuid.New()
	ev := req.Event
	var run db.Run
	err = s.store.Tx(ctx, func(q *db.Queries) error {
		var err error
		run, err = q.CreateRun(ctx, db.CreateRunParams{
			ID: runID, Repo: req.Repo, CloneUrl: req.CloneURL, EventType: string(ev.Type), Ref: ev.Ref,
			CommitSha: ev.SHA, Branch: ev.Branch, BaseBranch: ev.BaseBranch, Tag: ev.Tag,
			Pool: req.Pool, PipelineSource: string(req.Pipeline),
		})
		if err != nil {
			return err
		}
		for _, pj := range plan.Jobs {
			jobID := uuid.New()
			spec, err := proto.Marshal(jobSpec(jobID, runID, pj, req.CloneURL, ev))
			if err != nil {
				return err
			}
			labels := pj.RunsOn.Labels
			if labels == nil {
				labels = map[string]string{}
			}
			labelsJSON, err := json.Marshal(labels)
			if err != nil {
				return err
			}
			state := "queued"
			if len(pj.Needs) > 0 {
				state = "pending"
			}
			if err := q.CreateJob(ctx, db.CreateJobParams{
				ID: jobID, RunID: runID, Key: pj.ID, Name: pj.Name, Pool: req.Pool,
				Os: pj.RunsOn.OS, Arch: pj.RunsOn.Arch, Image: pj.RunsOn.Image, Labels: labelsJSON,
				Needs: nonNil(pj.Needs), Spec: spec, State: state,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return db.Run{}, fmt.Errorf("storing run: %w", err)
	}
	s.log.Info("run created", "run", runID, "repo", req.Repo, "sha", ev.SHA, "pool", req.Pool, "jobs", len(plan.Jobs))
	s.Kick()
	return run, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// jobSpec is what the worker receives, minus the per-attempt job token.
func jobSpec(jobID, runID uuid.UUID, pj *pipeline.PlannedJob, cloneURL string, ev pipeline.Event) *opificav1.Job {
	j := &opificav1.Job{
		Id:             jobID.String(),
		RunId:          runID.String(),
		Name:           pj.Name,
		Source:         &opificav1.Source{CloneUrl: cloneURL, CommitSha: ev.SHA, Ref: ev.Ref},
		Image:          pj.RunsOn.Image,
		Env:            pj.Env,
		AllowedSecrets: pj.Secrets,
		Timeout:        durationpb.New(pj.Timeout),
	}
	for _, st := range pj.Steps {
		step := &opificav1.Step{
			Name: st.Name, Env: st.Env, Secrets: st.Secrets, ContinueOnError: st.ContinueOnError,
		}
		if st.Timeout > 0 {
			step.Timeout = durationpb.New(st.Timeout)
		}
		switch {
		case st.Run != nil:
			step.Action = &opificav1.Step_Run{Run: &opificav1.RunAction{
				Command: st.Run.Command, Shell: st.Run.Shell, WorkingDirectory: st.Run.WorkingDirectory,
			}}
		case st.Report != nil:
			step.Action = &opificav1.Step_Report{Report: &opificav1.ReportAction{
				Format: opificav1.ReportFormat_REPORT_FORMAT_JUNIT, Paths: st.Report.Paths,
			}}
		case st.Upload != nil:
			step.Action = &opificav1.Step_Upload{Upload: &opificav1.UploadAction{Name: st.Upload.Name, Paths: st.Upload.Paths}}
		case st.Sign != nil:
			step.Action = &opificav1.Step_Sign{Sign: &opificav1.SignAction{Paths: st.Sign.Paths, Profile: st.Sign.Profile}}
		}
		j.Steps = append(j.Steps, step)
	}
	return j
}

// CancelRun stops a run: waiting jobs are cancelled at once, and workers
// running its jobs are told to stop.
func (s *Scheduler) CancelRun(ctx context.Context, runID uuid.UUID) error {
	if _, err := s.store.RequestRunCancel(ctx, runID); err != nil {
		if store.IsNotFound(err) {
			return fmt.Errorf("run %s not found or already finished", runID)
		}
		return err
	}
	if err := s.store.CancelWaitingJobs(ctx, runID); err != nil {
		return err
	}
	active, err := s.store.ActiveRunJobs(ctx, runID)
	if err != nil {
		return err
	}
	for _, j := range active {
		if j.WorkerID != nil {
			s.cancelOnWorker(ctx, *j.WorkerID, j.ID, "run cancelled")
		}
	}
	s.advance(ctx, runID)
	s.log.Info("run cancel requested", "run", runID, "running_jobs", len(active))
	return nil
}

// advance moves a run forward after a job finished: jobs that can no longer
// run are skipped, jobs whose needs are met are queued, and the run is
// closed when nothing is left.
func (s *Scheduler) advance(ctx context.Context, runID uuid.UUID) {
	for {
		skipped, err := s.store.SkipBlockedJobs(ctx, runID)
		if err != nil {
			s.log.Error("skipping blocked jobs", "run", runID, "err", err)
			return
		}
		if len(skipped) == 0 {
			break
		}
	}
	ready, err := s.store.QueueReadyJobs(ctx, runID)
	if err != nil {
		s.log.Error("queueing ready jobs", "run", runID, "err", err)
		return
	}
	if len(ready) > 0 {
		s.Kick()
	}
	run, err := s.store.FinishRunIfDone(ctx, runID)
	switch {
	case store.IsNotFound(err):
	case err != nil:
		s.log.Error("finishing run", "run", runID, "err", err)
	default:
		s.log.Info("run finished", "run", runID, "state", run.State)
	}
}

// ---------------------------------------------------------------- assignment

func (s *Scheduler) assign(ctx context.Context) {
	sessions := s.sessions.List()
	mrand.Shuffle(len(sessions), func(i, j int) { sessions[i], sessions[j] = sessions[j], sessions[i] })
	for _, w := range sessions {
		if len(w.Images) == 0 {
			continue
		}
		active, err := s.store.CountWorkerJobs(ctx, &w.WorkerID)
		if err != nil {
			s.log.Error("counting worker jobs", "worker", w.WorkerID, "err", err)
			return
		}
		for free := w.MaxJobs - int(active); free > 0; free-- {
			if !s.assignOne(ctx, w) {
				break
			}
		}
	}
}

// assignOne claims one job for w and sends it. It reports whether a job was sent.
func (s *Scheduler) assignOne(ctx context.Context, w workers.SessionInfo) bool {
	labels, err := json.Marshal(nonNilMap(w.Labels))
	if err != nil {
		return false
	}
	token, tokenHash, err := newJobToken()
	if err != nil {
		return false
	}
	lease := time.Now().Add(s.cfg.Lease)
	job, err := s.store.ClaimJob(ctx, db.ClaimJobParams{
		WorkerID: &w.WorkerID, LeaseExpiresAt: &lease, TokenHash: tokenHash,
		Pool: w.Pool, Os: w.OS, Arch: w.Arch, Images: w.Images, WorkerLabels: labels,
	})
	if store.IsNotFound(err) {
		return false
	}
	if err != nil {
		s.log.Error("claiming job", "worker", w.WorkerID, "err", err)
		return false
	}

	spec := &opificav1.Job{}
	if err := proto.Unmarshal(job.Spec, spec); err != nil {
		s.log.Error("stored job spec is invalid", "job", job.ID, "err", err)
		s.store.RetryJob(ctx, db.RetryJobParams{ID: job.ID, Error: "stored job spec is invalid"})
		return true
	}
	spec.JobToken = token

	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = s.sessions.Send(sendCtx, w.WorkerID, &opificav1.ConnectResponse{Message: &opificav1.ConnectResponse_AssignJob{
		AssignJob: &opificav1.AssignJob{Job: spec, Lease: durationpb.New(s.cfg.Lease)},
	}})
	if err != nil {
		s.log.Warn("could not deliver job; putting it back", "job", job.ID, "worker", w.WorkerID, "err", err)
		if err := s.store.UnclaimJob(ctx, db.UnclaimJobParams{ID: job.ID, WorkerID: &w.WorkerID}); err != nil {
			s.log.Error("unclaiming job", "job", job.ID, "err", err)
		}
		return false
	}
	s.log.Info("job assigned", "job", job.ID, "key", job.Key, "worker", w.WorkerID, "attempt", job.Attempt)
	return true
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// newJobToken returns a token for one job attempt and its hash. The token
// will scope the job's secret requests and uploads; only the hash is stored.
func newJobToken() (string, []byte, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, err
	}
	tok := "opt_" + base64.RawURLEncoding.EncodeToString(b)
	h := sha256.Sum256([]byte(tok))
	return tok, h[:], nil
}

func (s *Scheduler) reapLeases(ctx context.Context) {
	expired, err := s.store.ExpiredLeases(ctx)
	if err != nil {
		s.log.Error("finding expired leases", "err", err)
		return
	}
	for _, e := range expired {
		job, err := s.store.RetryJob(ctx, db.RetryJobParams{
			ID: e.ID, WorkerID: e.WorkerID, Error: "the worker stopped responding (lease expired)",
		})
		if store.IsNotFound(err) {
			continue // finished meanwhile
		}
		if err != nil {
			s.log.Error("retrying job", "job", e.ID, "err", err)
			continue
		}
		s.log.Warn("lease expired", "job", job.ID, "worker", e.WorkerID, "attempt", job.Attempt, "now", job.State)
		if e.WorkerID != nil {
			// In case the worker is alive but lost track of the job.
			s.cancelOnWorker(ctx, *e.WorkerID, job.ID, "lease expired")
		}
		s.settled(ctx, job)
	}
}

// settled continues after a job changed state through a retry.
func (s *Scheduler) settled(ctx context.Context, job db.Job) {
	if job.State == "queued" {
		s.Kick()
		return
	}
	s.advance(ctx, job.RunID)
}

// propagateCancels tells workers connected here to stop jobs of runs that
// were cancelled, possibly by another process.
func (s *Scheduler) propagateCancels(ctx context.Context) {
	jobs, err := s.store.JobsToCancel(ctx)
	if err != nil {
		s.log.Error("finding jobs to cancel", "err", err)
		return
	}
	for _, j := range jobs {
		if _, ok := s.sessions.Get(*j.WorkerID); ok {
			s.cancelOnWorker(ctx, *j.WorkerID, j.ID, "run cancelled")
		}
	}
}

func (s *Scheduler) cancelOnWorker(ctx context.Context, worker, job uuid.UUID, reason string) {
	sendCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := s.sessions.Send(sendCtx, worker, &opificav1.ConnectResponse{Message: &opificav1.ConnectResponse_CancelJob{
		CancelJob: &opificav1.CancelJob{JobId: job.String(), Reason: reason},
	}})
	if err != nil && !errors.Is(err, workers.ErrNotConnected) {
		s.log.Warn("could not send cancel", "job", job, "worker", worker, "err", err)
	}
}

// ---------------------------------------------------------------- worker messages

// WorkerConnected offers work to a newly connected worker.
func (s *Scheduler) WorkerConnected(uuid.UUID) { s.Kick() }

// WorkerDisconnected puts back jobs the worker never reported starting.
func (s *Scheduler) WorkerDisconnected(worker uuid.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ids, err := s.store.ReleaseUnstartedJobs(ctx, &worker)
	if err != nil {
		s.log.Error("releasing unstarted jobs", "worker", worker, "err", err)
		return
	}
	if len(ids) > 0 {
		s.log.Info("worker disconnected before starting jobs; queued them again", "worker", worker, "jobs", len(ids))
		s.Kick()
	}
}

// HandleWorkerMessage handles heartbeats and job updates. Database errors are
// logged rather than returned, so a database hiccup doesn't drop workers.
func (s *Scheduler) HandleWorkerMessage(ctx context.Context, worker uuid.UUID, msg *opificav1.ConnectRequest) error {
	switch m := msg.GetMessage().(type) {
	case *opificav1.ConnectRequest_Heartbeat:
		s.renew(ctx, worker, m.Heartbeat.GetRunningJobIds())
	case *opificav1.ConnectRequest_JobUpdate:
		s.jobUpdate(ctx, worker, m.JobUpdate)
	case *opificav1.ConnectRequest_SecretRequest:
		// Until the secrets broker exists, answer so the job doesn't hang.
		return s.sessions.Send(ctx, worker, &opificav1.ConnectResponse{Message: &opificav1.ConnectResponse_SecretResponse{
			SecretResponse: &opificav1.SecretResponse{
				RequestId: m.SecretRequest.GetRequestId(),
				Result:    &opificav1.SecretResponse_DeniedReason{DeniedReason: "secrets are not implemented yet"},
			},
		}})
	case *opificav1.ConnectRequest_LogChunk:
		// Log storage comes next; drop for now.
	}
	return nil
}

func (s *Scheduler) renew(ctx context.Context, worker uuid.UUID, ids []string) {
	if len(ids) == 0 {
		return
	}
	jobIDs := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if u, err := uuid.Parse(id); err == nil {
			jobIDs = append(jobIDs, u)
		}
	}
	lease := time.Now().Add(s.cfg.Lease)
	if err := s.store.RenewLeases(ctx, db.RenewLeasesParams{LeaseExpiresAt: &lease, WorkerID: &worker, JobIds: jobIDs}); err != nil {
		s.log.Error("renewing leases", "worker", worker, "err", err)
	}
}

func (s *Scheduler) jobUpdate(ctx context.Context, worker uuid.UUID, u *opificav1.JobUpdate) {
	jobID, err := uuid.Parse(u.GetJobId())
	if err != nil {
		s.log.Warn("job update with invalid job id", "worker", worker, "job", u.GetJobId())
		return
	}
	log := s.log.With("job", jobID, "worker", worker)

	switch u.GetState() {
	case opificav1.JobState_JOB_STATE_PREPARING, opificav1.JobState_JOB_STATE_RUNNING, opificav1.JobState_JOB_STATE_UPLOADING:
		if _, err := s.store.MarkJobRunning(ctx, db.MarkJobRunningParams{ID: jobID, WorkerID: &worker}); err != nil {
			s.stale(ctx, log, worker, jobID, err)
		}

	case opificav1.JobState_JOB_STATE_SUCCEEDED, opificav1.JobState_JOB_STATE_FAILED, opificav1.JobState_JOB_STATE_CANCELLED:
		state := map[opificav1.JobState]string{
			opificav1.JobState_JOB_STATE_SUCCEEDED: "succeeded",
			opificav1.JobState_JOB_STATE_FAILED:    "failed",
			opificav1.JobState_JOB_STATE_CANCELLED: "cancelled",
		}[u.GetState()]
		job, err := s.store.FinishJob(ctx, db.FinishJobParams{
			ID: jobID, WorkerID: &worker, State: state, ExitCode: u.ExitCode, Error: u.GetError(),
		})
		if err != nil {
			s.stale(ctx, log, worker, jobID, err)
			return
		}
		log.Info("job finished", "key", job.Key, "state", state)
		s.advance(ctx, job.RunID)

	case opificav1.JobState_JOB_STATE_ERRORED:
		job, err := s.store.RetryJob(ctx, db.RetryJobParams{ID: jobID, WorkerID: &worker, Error: u.GetError()})
		if err != nil {
			s.stale(ctx, log, worker, jobID, err)
			return
		}
		log.Warn("job errored on the worker", "err", u.GetError(), "attempt", job.Attempt, "now", job.State)
		s.settled(ctx, job)

	default:
		log.Warn("job update with unexpected state", "state", u.GetState())
	}
}

// stale handles an update for a job this worker no longer holds, e.g. after
// its lease expired and the job went to another worker: tell it to stop.
func (s *Scheduler) stale(ctx context.Context, log *slog.Logger, worker, jobID uuid.UUID, err error) {
	if !store.IsNotFound(err) {
		log.Error("recording job update", "err", err)
		return
	}
	log.Warn("ignoring update for a job this worker no longer holds")
	s.cancelOnWorker(ctx, worker, jobID, "this job is no longer assigned to this worker")
}
