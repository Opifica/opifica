package integration

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/pipeline"
	"github.com/opifica/opifica/pkg/worker"
)

// outcome is what a fake worker does with a job.
type outcome struct {
	state opificav1.JobState // final state to report
	exit  int32
	delay time.Duration
	// silent: accept the job, then never mention it again, like a worker
	// that crashed right after receiving it.
	silent bool
	// vanish: report the job running, then never mention it again, like a
	// worker that crashed mid-job.
	vanish bool
	// hang: run until cancelled.
	hang bool
}

func succeed() outcome { return outcome{state: opificav1.JobState_JOB_STATE_SUCCEEDED} }

// fakeWorker is a real worker client whose "jobs" follow a script.
type fakeWorker struct {
	*runningClient
	name string
	id   *worker.Identity

	mu         sync.Mutex
	running    map[string]chan struct{} // job ID -> cancel
	received   []*opificav1.Job
	maxRunning int
	behave     func(*opificav1.Job) outcome
}

type workerOpts struct {
	pool    string
	os      opificav1.OperatingSystem
	images  []string
	labels  map[string]string
	maxJobs uint32
	behave  func(*opificav1.Job) outcome
}

func (e *env) fakeWorker(name string, o workerOpts) *fakeWorker {
	e.t.Helper()
	if o.pool == "" {
		o.pool = "trusted"
	}
	if o.os == 0 {
		o.os = opificav1.OperatingSystem_OPERATING_SYSTEM_LINUX
	}
	if o.images == nil {
		o.images = []string{"default"}
	}
	if o.maxJobs == 0 {
		o.maxJobs = 4
	}
	if o.behave == nil {
		o.behave = func(*opificav1.Job) outcome { return succeed() }
	}
	wi := info()
	wi.Hostname, wi.Os, wi.MaxConcurrentJobs, wi.Labels, wi.Images = name, o.os, o.maxJobs, o.labels, nil
	for _, img := range o.images {
		wi.Images = append(wi.Images, &opificav1.ImageRef{Name: img})
	}

	id := e.joinWith(o.pool, wi)
	fw := &fakeWorker{name: name, id: id, running: map[string]chan struct{}{}, behave: o.behave}
	fw.runningClient = e.start(id, func(c *worker.ClientConfig) {
		c.Info = wi
		c.Status = fw.status
		c.OnMessage = fw.onMessage
	})
	return fw
}

func (fw *fakeWorker) status() (uint32, []string) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	ids := make([]string, 0, len(fw.running))
	for id := range fw.running {
		ids = append(ids, id)
	}
	return 0, ids
}

func (fw *fakeWorker) onMessage(_ context.Context, m *opificav1.ConnectResponse) {
	switch msg := m.GetMessage().(type) {
	case *opificav1.ConnectResponse_AssignJob:
		job := msg.AssignJob.GetJob()
		out := fw.behave(job)
		fw.mu.Lock()
		fw.received = append(fw.received, job)
		if out.silent {
			fw.mu.Unlock()
			return
		}
		cancel := make(chan struct{})
		fw.running[job.GetId()] = cancel
		fw.maxRunning = max(fw.maxRunning, len(fw.running))
		fw.mu.Unlock()
		go fw.run(job, out, cancel)
	case *opificav1.ConnectResponse_CancelJob:
		fw.mu.Lock()
		if c, ok := fw.running[msg.CancelJob.GetJobId()]; ok {
			close(c)
		}
		fw.mu.Unlock()
	}
}

func (fw *fakeWorker) update(job string, state opificav1.JobState, exit *int32) {
	fw.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_JobUpdate{JobUpdate: &opificav1.JobUpdate{
		JobId: job, State: state, ExitCode: exit,
	}}})
}

func (fw *fakeWorker) run(job *opificav1.Job, out outcome, cancel chan struct{}) {
	fw.update(job.GetId(), opificav1.JobState_JOB_STATE_RUNNING, nil)
	if out.vanish {
		fw.mu.Lock()
		delete(fw.running, job.GetId()) // stops renewing the lease
		fw.mu.Unlock()
		return
	}
	final := out.state
	var wait <-chan time.Time
	if !out.hang {
		wait = time.After(out.delay)
	}
	select {
	case <-wait:
	case <-cancel:
		final = opificav1.JobState_JOB_STATE_CANCELLED
	}
	fw.mu.Lock()
	delete(fw.running, job.GetId())
	fw.mu.Unlock()
	exit := out.exit
	fw.update(job.GetId(), final, &exit)
}

func (fw *fakeWorker) jobs() []*opificav1.Job {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return slices.Clone(fw.received)
}

func (e *env) createRun(src string, ev pipeline.Event, pool string) db.Run {
	e.t.Helper()
	if ev.Type == "" {
		ev = pipeline.Event{Type: pipeline.EventPush, Branch: "main", Ref: "refs/heads/main"}
	}
	if ev.SHA == "" {
		ev.SHA = "0123456789abcdef0123456789abcdef01234567"
	}
	if pool == "" {
		pool = "trusted"
	}
	run, err := e.sched.CreateRun(context.Background(), scheduler.RunRequest{
		Repo: "acme/app", CloneURL: "https://forge.example/acme/app.git", Event: ev, Pool: pool, Pipeline: []byte(src),
	})
	if err != nil {
		e.t.Fatalf("CreateRun: %v", err)
	}
	return run
}

// waitRun waits for the run to finish and returns it with its jobs by key.
func (e *env) waitRun(id uuid.UUID) (db.Run, map[string]db.Job) {
	e.t.Helper()
	ctx := context.Background()
	var run db.Run
	eventually(e.t, "run to finish", func() bool {
		var err error
		run, err = e.svc.Store.GetRun(ctx, id)
		return err == nil && run.State != "running"
	})
	jobs, err := e.svc.Store.ListRunJobs(ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	byKey := map[string]db.Job{}
	for _, j := range jobs {
		byKey[j.Key] = j
	}
	return run, byKey
}

func TestSingleJobRun(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("linux-1", workerOpts{})
	run := e.createRun(`
on: push
jobs:
  build:
    runs-on: linux
    steps:
      - run: make
`, pipeline.Event{}, "")

	got, jobs := e.waitRun(run.ID)
	if got.State != "succeeded" {
		t.Errorf("run state = %s", got.State)
	}
	if j := jobs["build"]; j.State != "succeeded" || j.Attempt != 1 || *j.ExitCode != 0 {
		t.Errorf("job = state %s attempt %d", j.State, j.Attempt)
	}

	// The worker received the full job spec with a fresh token.
	received := w.jobs()
	if len(received) != 1 {
		t.Fatalf("worker received %d jobs", len(received))
	}
	spec := received[0]
	if spec.GetSource().GetCommitSha() != run.CommitSha || spec.GetSource().GetCloneUrl() == "" {
		t.Errorf("source = %v", spec.GetSource())
	}
	if !strings.HasPrefix(spec.GetJobToken(), "opt_") {
		t.Errorf("job token = %q", spec.GetJobToken())
	}
	if spec.GetSteps()[0].GetRun().GetCommand() != "make" {
		t.Errorf("steps = %v", spec.GetSteps())
	}
}

// Each matrix job lands on a worker of its OS; release waits for all of them.
func TestMatrixAcrossOperatingSystems(t *testing.T) {
	e := newEnv(t)
	slow := func(*opificav1.Job) outcome { o := succeed(); o.delay = 30 * time.Millisecond; return o }
	linux := e.fakeWorker("linux", workerOpts{behave: slow})
	windows := e.fakeWorker("windows", workerOpts{os: opificav1.OperatingSystem_OPERATING_SYSTEM_WINDOWS, behave: slow})
	macos := e.fakeWorker("macos", workerOpts{os: opificav1.OperatingSystem_OPERATING_SYSTEM_MACOS, behave: slow,
		images: []string{"default", "xcode-16"}})

	run := e.createRun(`
on: push
jobs:
  test:
    matrix: { os: [linux, windows, macos] }
    runs-on:
      os: ${{ matrix.os }}
    steps: [{ run: make test }]
  release:
    needs: test
    runs-on: { os: macos, image: xcode-16 }
    steps: [{ run: make release }]
`, pipeline.Event{}, "")

	got, jobs := e.waitRun(run.ID)
	if got.State != "succeeded" {
		t.Fatalf("run state = %s", got.State)
	}
	for w, want := range map[*fakeWorker][]string{
		linux:   {"test (linux)"},
		windows: {"test (windows)"},
		macos:   {"test (macos)", "release"},
	} {
		var names []string
		for _, j := range w.jobs() {
			names = append(names, j.GetName())
		}
		slices.Sort(names)
		slices.Sort(want)
		if !slices.Equal(names, want) {
			t.Errorf("%s ran %v, want %v", w.name, names, want)
		}
	}
	rel := jobs["release"]
	for _, k := range []string{"test[os=linux]", "test[os=windows]", "test[os=macos]"} {
		if !jobs[k].FinishedAt.Before(*rel.StartedAt) && !jobs[k].FinishedAt.Equal(*rel.StartedAt) {
			t.Errorf("release started before %s finished", k)
		}
	}
}

func TestFailureSkipsDependents(t *testing.T) {
	e := newEnv(t)
	e.fakeWorker("linux", workerOpts{behave: func(j *opificav1.Job) outcome {
		if j.GetName() == "test" {
			return outcome{state: opificav1.JobState_JOB_STATE_FAILED, exit: 2}
		}
		return succeed()
	}})
	run := e.createRun(`
on: push
jobs:
  test:
    runs-on: linux
    steps: [{ run: make test }]
  lint:
    runs-on: linux
    steps: [{ run: make lint }]
  package:
    needs: test
    runs-on: linux
    steps: [{ run: make package }]
  deploy:
    needs: package
    runs-on: linux
    steps: [{ run: make deploy }]
`, pipeline.Event{}, "")

	got, jobs := e.waitRun(run.ID)
	if got.State != "failed" {
		t.Errorf("run state = %s, want failed", got.State)
	}
	want := map[string]string{"test": "failed", "lint": "succeeded", "package": "skipped", "deploy": "skipped"}
	for k, st := range want {
		if jobs[k].State != st {
			t.Errorf("%s = %s, want %s", k, jobs[k].State, st)
		}
	}
	if *jobs["test"].ExitCode != 2 {
		t.Errorf("exit code = %d", *jobs["test"].ExitCode)
	}
}

// A release job never runs on a worker from a less trusted pool. The pool
// comes from the run (the controller's routing), not from the pipeline.
func TestPoolsAreIsolated(t *testing.T) {
	e := newEnv(t)
	untrusted := e.fakeWorker("pr-builder", workerOpts{pool: "untrusted"})
	run := e.createRun(`
on: { tag: { pattern: "v*" } }
jobs:
  release:
    runs-on: linux
    steps: [{ run: make release }]
`, pipeline.Event{Type: pipeline.EventTag, Tag: "v1.0.0", Ref: "refs/tags/v1.0.0"}, "release")

	time.Sleep(200 * time.Millisecond)
	if n := len(untrusted.jobs()); n != 0 {
		t.Fatalf("untrusted worker got %d release jobs", n)
	}
	release := e.fakeWorker("release-builder", workerOpts{pool: "release"})
	got, _ := e.waitRun(run.ID)
	if got.State != "succeeded" || len(release.jobs()) != 1 || len(untrusted.jobs()) != 0 {
		t.Errorf("run %s; release worker ran %d, untrusted ran %d", got.State, len(release.jobs()), len(untrusted.jobs()))
	}
}

func TestImagesAndLabelsMatch(t *testing.T) {
	e := newEnv(t)
	plain := e.fakeWorker("plain", workerOpts{})
	run := e.createRun(`
on: push
jobs:
  gpu:
    runs-on:
      os: linux
      image: cuda
      labels: { gpu: "true" }
    steps: [{ run: nvidia-smi }]
`, pipeline.Event{}, "")
	time.Sleep(200 * time.Millisecond)
	if n := len(plain.jobs()); n != 0 {
		t.Fatalf("worker without the image or label got the job")
	}
	// Right label, wrong image: still no.
	labelOnly := e.fakeWorker("label-only", workerOpts{labels: map[string]string{"gpu": "true"}})
	time.Sleep(200 * time.Millisecond)
	if n := len(labelOnly.jobs()); n != 0 {
		t.Fatalf("worker without the image got the job")
	}
	// Right image, missing label: still no.
	imageOnly := e.fakeWorker("image-only", workerOpts{images: []string{"cuda"}, labels: map[string]string{"gpu": "false"}})
	time.Sleep(200 * time.Millisecond)
	if n := len(imageOnly.jobs()); n != 0 {
		t.Fatalf("worker without the gpu=true label got the job")
	}
	gpu := e.fakeWorker("gpu", workerOpts{images: []string{"cuda"}, labels: map[string]string{"gpu": "true", "vram": "24g"}})
	got, _ := e.waitRun(run.ID)
	if got.State != "succeeded" || len(gpu.jobs()) != 1 {
		t.Errorf("run = %s, gpu worker ran %d jobs", got.State, len(gpu.jobs()))
	}
}

func TestCapacityIsRespected(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("mac", workerOpts{maxJobs: 2, behave: func(*opificav1.Job) outcome {
		o := succeed()
		o.delay = 50 * time.Millisecond
		return o
	}})
	run := e.createRun(`
on: push
jobs:
  t:
    matrix: { n: ["1", "2", "3", "4", "5", "6"] }
    runs-on: linux
    steps: [{ run: make }]
`, pipeline.Event{}, "")
	got, _ := e.waitRun(run.ID)
	if got.State != "succeeded" {
		t.Fatalf("run = %s", got.State)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.maxRunning > 2 {
		t.Errorf("worker ran %d jobs at once, capacity 2", w.maxRunning)
	}
	if len(w.received) != 6 {
		t.Errorf("worker ran %d jobs, want 6", len(w.received))
	}
}

// A worker starts a job and crashes. When the lease expires the job goes to
// another worker. When the crashed worker comes back and reports a result
// for the job, it is ignored.
func TestExpiredLeaseIsRetriedElsewhere(t *testing.T) {
	e := newEnv(t)
	flaky := e.fakeWorker("flaky", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{vanish: true} }})

	run := e.createRun("on: push\njobs:\n  build:\n    runs-on: linux\n    steps: [{ run: make }]\n", pipeline.Event{}, "")
	eventually(t, "flaky worker to start the job", func() bool {
		j, err := e.svc.Store.ListRunJobs(context.Background(), run.ID)
		return err == nil && j[0].State == "running"
	})
	flaky.cancel() // the crash
	flaky.done <- <-flaky.done

	healthy := e.fakeWorker("healthy", workerOpts{})
	got, jobs := e.waitRun(run.ID)
	if got.State != "succeeded" {
		t.Fatalf("run = %s (job: %s, %q)", got.State, jobs["build"].State, jobs["build"].Error)
	}
	if len(healthy.jobs()) != 1 || jobs["build"].Attempt != 2 {
		t.Errorf("healthy ran %d jobs, attempt %d (want 1 and 2)", len(healthy.jobs()), jobs["build"].Attempt)
	}

	// The crashed worker restarts and reports the job it lost track of.
	restarted := e.start(flaky.id, nil)
	select {
	case <-restarted.Connected():
	case <-time.After(5 * time.Second):
		t.Fatal("restarted worker did not connect")
	}
	exit := int32(99)
	restarted.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_JobUpdate{JobUpdate: &opificav1.JobUpdate{
		JobId: jobs["build"].ID.String(), State: opificav1.JobState_JOB_STATE_FAILED, ExitCode: &exit,
	}}})
	// It is told to stop the job it no longer holds.
	select {
	case m := <-restarted.msgs:
		if m.GetCancelJob().GetJobId() != jobs["build"].ID.String() {
			t.Errorf("restarted worker got %v, want a cancel for the stale job", m)
		}
	case <-time.After(5 * time.Second):
		t.Error("stale update was not answered with a cancel")
	}
	j, _ := e.svc.Store.GetJob(context.Background(), jobs["build"].ID)
	if j.State != "succeeded" || *j.ExitCode != 0 {
		t.Errorf("a stale update changed the job: %s exit %d", j.State, *j.ExitCode)
	}
}

// A worker disconnects after being given a job it never started: the job
// is queued again at once, without waiting for the lease and without
// using up an attempt.
func TestUnstartedJobsReturnOnDisconnect(t *testing.T) {
	e := newEnv(t)
	flaky := e.fakeWorker("flaky", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{silent: true} }})
	run := e.createRun("on: push\njobs:\n  build:\n    runs-on: linux\n    steps: [{ run: make }]\n", pipeline.Event{}, "")
	eventually(t, "flaky worker to receive the job", func() bool { return len(flaky.jobs()) == 1 })
	flaky.cancel()
	flaky.done <- <-flaky.done

	e.fakeWorker("healthy", workerOpts{})
	got, jobs := e.waitRun(run.ID)
	if got.State != "succeeded" || jobs["build"].Attempt != 1 {
		t.Errorf("run %s, attempt %d (want succeeded on attempt 1)", got.State, jobs["build"].Attempt)
	}
}

func TestErroredJobsRetryThenFail(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("broken", workerOpts{behave: func(*opificav1.Job) outcome {
		return outcome{state: opificav1.JobState_JOB_STATE_ERRORED}
	}})
	run := e.createRun("on: push\njobs:\n  build:\n    runs-on: linux\n    steps: [{ run: make }]\n", pipeline.Event{}, "")
	got, jobs := e.waitRun(run.ID)
	if got.State != "failed" || jobs["build"].State != "failed" {
		t.Errorf("run = %s, job = %s", got.State, jobs["build"].State)
	}
	if n := len(w.jobs()); n != 3 || jobs["build"].Attempt != 3 {
		t.Errorf("job tried %d times (attempt %d), want 3", n, jobs["build"].Attempt)
	}
}

func TestCancelRun(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("linux", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{hang: true} }})
	run := e.createRun(`
on: push
jobs:
  long:
    runs-on: linux
    steps: [{ run: sleep 3600 }]
  after:
    needs: long
    runs-on: linux
    steps: [{ run: echo done }]
`, pipeline.Event{}, "")
	eventually(t, "the long job to start", func() bool {
		j, err := e.svc.Store.ListRunJobs(context.Background(), run.ID)
		return err == nil && slices.ContainsFunc(j, func(j db.Job) bool { return j.State == "running" })
	})

	if err := e.sched.CancelRun(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	got, jobs := e.waitRun(run.ID)
	if got.State != "cancelled" || jobs["long"].State != "cancelled" || jobs["after"].State != "cancelled" {
		t.Errorf("run %s, long %s, after %s", got.State, jobs["long"].State, jobs["after"].State)
	}
	if len(w.jobs()) != 1 {
		t.Errorf("worker got %d jobs after cancel", len(w.jobs()))
	}
	if err := e.sched.CancelRun(context.Background(), run.ID); err == nil {
		t.Error("cancelling a finished run succeeded")
	}
}

// Two controllers share one database: every job runs exactly once.
func TestTwoControllersNeverDoubleAssign(t *testing.T) {
	e := newEnv(t)
	second := scheduler.New(e.svc.Store, e.svc.Sessions(), quiet(), scheduler.Config{Lease: 400 * time.Millisecond, Interval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go second.Run(ctx)

	var workers []*fakeWorker
	for i := range 4 {
		workers = append(workers, e.fakeWorker("w"+string(rune('a'+i)), workerOpts{maxJobs: 3}))
	}
	run := e.createRun(`
on: push
jobs:
  t:
    matrix: { a: ["1","2","3","4","5"], b: ["1","2","3","4"] }
    runs-on: linux
    steps: [{ run: make }]
`, pipeline.Event{}, "")
	got, jobs := e.waitRun(run.ID)
	if got.State != "succeeded" {
		t.Fatalf("run = %s", got.State)
	}
	seen := map[string]int{}
	for _, w := range workers {
		for _, j := range w.jobs() {
			seen[j.GetId()]++
		}
	}
	for k, j := range jobs {
		if seen[j.ID.String()] != 1 {
			t.Errorf("%s ran %d times", k, seen[j.ID.String()])
		}
	}
}

func TestCreateRunRejects(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ev := pipeline.Event{Type: pipeline.EventPush, Branch: "develop", SHA: "abc"}
	for name, tc := range map[string]struct {
		src  string
		want error
	}{
		"not triggered": {"on: { push: { branches: [main] } }\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n", scheduler.ErrNotTriggered},
		"all skipped":   {"on: push\njobs:\n  a:\n    if: event.tag != \"\"\n    runs-on: linux\n    steps: [{run: x}]\n", scheduler.ErrNothingToRun},
		"invalid":       {"on: push\njobs: {}\n", nil},
	} {
		_, err := e.sched.CreateRun(ctx, scheduler.RunRequest{Repo: "r", Pool: "trusted", Event: ev, Pipeline: []byte(tc.src)})
		if err == nil || tc.want != nil && err != tc.want {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
	runs, _ := e.svc.Store.ListRuns(ctx, 10)
	if len(runs) != 0 {
		t.Errorf("%d runs stored from rejected requests", len(runs))
	}
}

// `controller run cancel` runs in its own process with no worker sessions.
// The serving controller must still stop the job on its worker.
func TestCancelFromAnotherProcess(t *testing.T) {
	e := newEnv(t)
	e.fakeWorker("linux", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{hang: true} }})
	run := e.createRun("on: push\njobs:\n  long:\n    runs-on: linux\n    steps: [{ run: sleep 3600 }]\n", pipeline.Event{}, "")
	eventually(t, "the job to start", func() bool {
		j, err := e.svc.Store.ListRunJobs(context.Background(), run.ID)
		return err == nil && len(j) == 1 && j[0].State == "running"
	})

	cli := scheduler.New(e.svc.Store, workers.NewRegistry(), quiet(), scheduler.Config{})
	if err := cli.CancelRun(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	got, jobs := e.waitRun(run.ID)
	if got.State != "cancelled" || jobs["long"].State != "cancelled" {
		t.Errorf("run %s, job %s", got.State, jobs["long"].State)
	}
}
