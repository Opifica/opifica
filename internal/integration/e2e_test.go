package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/pipeline"
	"github.com/opifica/opifica/pkg/worker"
	"github.com/opifica/opifica/pkg/worker/workertest"
)

// These tests run the whole path for real: controller, scheduler, the worker
// runtime, a Git mirror, and a real agent per job running real commands. Only
// the VM is faked: each "VM" is a fresh directory with its own agent.

func hostOS() string {
	if runtime.GOOS == "darwin" {
		return "macos"
	}
	return runtime.GOOS
}

type realWorker struct {
	hv     *workertest.Hypervisor
	cancel context.CancelFunc
	done   chan error
}

func (e *env) realWorker(capacity int) *realWorker {
	e.t.Helper()
	if runtime.GOOS == "windows" {
		e.t.Skip("the test pipelines use a POSIX shell")
	}
	if _, err := exec.LookPath("git"); err != nil {
		e.t.Skip("git is not installed")
	}
	pool := worker.PoolConfig{
		Capacity: capacity,
		Images:   map[string]worker.Image{"default": {Name: "default"}},
		Warm:     map[string]int{"default": 1},
	}
	dir := e.t.TempDir()
	e.joinInto("trusted", worker.Info(pool, nil), dir)

	rw := &realWorker{hv: &workertest.Hypervisor{Agent: true}, done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	rw.cancel = cancel
	go func() {
		rw.done <- worker.Run(ctx, worker.Config{
			ControllerURL: e.url, StateDir: dir, Hypervisor: rw.hv, Pool: pool,
			AllowLocalRepos: true, HeartbeatInterval: 50 * time.Millisecond, Logger: quiet(),
		})
	}()
	e.t.Cleanup(func() {
		cancel()
		<-rw.done
		if n := rw.hv.Alive(); n != 0 {
			e.t.Errorf("%d VMs still alive after the worker stopped", n)
		}
	})
	return rw
}

// gitRepo creates a repository with the given files and returns its path
// and commit.
func gitRepo(t *testing.T, files map[string]string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitIn := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com", "GIT_CONFIG_GLOBAL="+os.DevNull)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	gitIn("init", "-q", "-b", "main")
	gitIn("add", ".")
	gitIn("commit", "-q", "-m", "test")
	return dir, gitIn("rev-parse", "HEAD")
}

func (e *env) startRun(repo, sha, src string) uuid.UUID {
	e.t.Helper()
	run, err := e.sched.CreateRun(context.Background(), runRequest(repo, sha, src))
	if err != nil {
		e.t.Fatalf("CreateRun: %v", err)
	}
	return run.ID
}

func (e *env) jobLog(runID uuid.UUID, key string) string {
	e.t.Helper()
	jobs, err := e.svc.Store.ListRunJobs(context.Background(), runID)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, j := range jobs {
		if j.Key == key {
			r, err := e.logs.Open(j.ID)
			if err != nil {
				return ""
			}
			defer r.Close()
			data, _ := io.ReadAll(r)
			return string(data)
		}
	}
	e.t.Fatalf("no job %q", key)
	return ""
}

func wantIn(t *testing.T, log string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(log, p) {
			t.Errorf("log lacks %q:\n%s", p, log)
		}
	}
}

func TestE2ERealCommands(t *testing.T) {
	e := newEnv(t)
	e.realWorker(2)
	repo, sha := gitRepo(t, map[string]string{"README": "hello world\n", "scripts/check.sh": "echo checked in $(pwd)\n"})

	runID := e.startRun(repo, sha, `
on: push
env:
  GREETING: hi
jobs:
  build:
    runs-on: `+hostOS()+`
    steps:
      - run: |
          echo "$GREETING, the readme says: $(cat README)"
          test "$OPIFICA_COMMIT_SHA" = "`+sha+`"
          test "$CI" = true
      - run: echo to-stderr >&2
      - run: sh check.sh
        working-directory: scripts
  check:
    needs: build
    runs-on: `+hostOS()+`
    steps:
      - name: allowed to fail
        run: exit 7
        continue-on-error: true
      - run: echo still running
      - report: junit build/*.xml
`)
	run, jobs := e.waitRun(runID)
	if run.State != "succeeded" {
		t.Fatalf("run %s; build %s (%q), check %s (%q)\n%s", run.State, jobs["build"].State, jobs["build"].Error,
			jobs["check"].State, jobs["check"].Error, e.jobLog(runID, "build"))
	}
	wantIn(t, e.jobLog(runID, "build"),
		"==> Fetching", "==> Starting a fresh VM", "==> Step 1/3",
		"hi, the readme says: hello world", "to-stderr", "checked in ", "/src/scripts")
	wantIn(t, e.jobLog(runID, "check"),
		"Step failed with exit code 7; continuing", "still running", "Test reports are not stored yet")
}

func TestE2EFailure(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, `
on: push
jobs:
  test:
    runs-on: `+hostOS()+`
    steps:
      - run: echo before; exit 7
      - run: echo never
  deploy:
    needs: test
    runs-on: `+hostOS()+`
    steps: [{ run: echo deploying }]
`)
	run, jobs := e.waitRun(runID)
	if run.State != "failed" || jobs["test"].State != "failed" || *jobs["test"].ExitCode != 7 || jobs["deploy"].State != "skipped" {
		t.Fatalf("run %s; test %s exit %v; deploy %s", run.State, jobs["test"].State, jobs["test"].ExitCode, jobs["deploy"].State)
	}
	if jobs["test"].Attempt != 1 {
		t.Errorf("a failing command was retried (attempt %d); only infrastructure errors should be", jobs["test"].Attempt)
	}
	log := e.jobLog(runID, "test")
	wantIn(t, log, "before", "Step failed with exit code 7")
	if strings.Contains(log, "never") {
		t.Error("ran the step after the failing one")
	}
}

// Every job gets a fresh VM: nothing one job writes is visible to the next.
func TestE2EFreshVMPerJob(t *testing.T) {
	e := newEnv(t)
	rw := e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, `
on: push
jobs:
  first:
    runs-on: `+hostOS()+`
    steps: [{ run: "test ! -e ../left-behind && touch ../left-behind README.modified" }]
  second:
    needs: first
    runs-on: `+hostOS()+`
    steps: [{ run: "test ! -e ../left-behind && test ! -e README.modified" }]
`)
	run, jobs := e.waitRun(runID)
	if run.State != "succeeded" {
		t.Fatalf("run %s: first %s, second %s\n%s", run.State, jobs["first"].State, jobs["second"].State, e.jobLog(runID, "second"))
	}
	if created, _ := rw.hv.Counts(); created < 2 {
		t.Errorf("only %d VMs created for 2 jobs", created)
	}
}

func TestE2ESecretDenied(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, `
on: push
jobs:
  deploy:
    runs-on: `+hostOS()+`
    secrets: [DEPLOY_TOKEN]
    steps:
      - run: echo deploying
        secrets: [DEPLOY_TOKEN]
`)
	run, jobs := e.waitRun(runID)
	j := jobs["deploy"]
	if run.State != "failed" || !strings.Contains(j.Error, "secret DEPLOY_TOKEN was denied") || j.Attempt != 1 {
		t.Errorf("run %s; job %s, attempt %d, error %q", run.State, j.State, j.Attempt, j.Error)
	}
	// "deploying" alone on a line is the command's output; the step's header
	// line ("==> Step 1/1: echo deploying") doesn't count.
	if regexp.MustCompile(`(?m)^deploying$`).MatchString(e.jobLog(runID, "deploy")) {
		t.Error("the step ran without its secret")
	}
}

func TestE2ECancelStopsTheCommand(t *testing.T) {
	e := newEnv(t)
	rw := e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, "on: push\njobs:\n  long:\n    runs-on: "+hostOS()+"\n    steps: [{ run: echo started; sleep 300 }]\n")
	eventually(t, "the command to start", func() bool { return strings.Contains(e.jobLog(runID, "long"), "started") })

	start := time.Now()
	if err := e.sched.CancelRun(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	run, jobs := e.waitRun(runID)
	if run.State != "cancelled" || jobs["long"].State != "cancelled" {
		t.Errorf("run %s, job %s", run.State, jobs["long"].State)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("cancelling took %v", d)
	}
	eventually(t, "the job's VM to be destroyed", func() bool { return rw.hv.Alive() <= 1 }) // the warm one may remain
}

func TestE2EJobTimeout(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, "on: push\njobs:\n  slow:\n    runs-on: "+hostOS()+"\n    timeout: 2s\n    steps: [{ run: sleep 60 }]\n")
	run, jobs := e.waitRun(runID)
	j := jobs["slow"]
	if run.State != "failed" || j.State != "failed" || !strings.Contains(j.Error+e.jobLog(runID, "slow"), "timed out") {
		t.Errorf("run %s; job %s, error %q\n%s", run.State, j.State, j.Error, e.jobLog(runID, "slow"))
	}
}

func TestE2EMissingCommitFailsWithoutRetry(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, _ := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, strings.Repeat("ab", 20), "on: push\njobs:\n  a:\n    runs-on: "+hostOS()+"\n    steps: [{ run: echo hi }]\n")
	run, jobs := e.waitRun(runID)
	if run.State != "failed" || jobs["a"].Attempt != 1 || !strings.Contains(jobs["a"].Error, "commit not found") {
		t.Errorf("run %s; attempt %d; error %q", run.State, jobs["a"].Attempt, jobs["a"].Error)
	}
}

func runRequest(repo, sha, src string) scheduler.RunRequest {
	return scheduler.RunRequest{
		Repo: "acme/app", CloneURL: repo, Pool: "trusted", Pipeline: []byte(src),
		Event: pipeline.Event{Type: pipeline.EventPush, Branch: "main", Ref: "refs/heads/main", SHA: sha},
	}
}

// The connection drops while a job runs. The job keeps running, the worker
// reconnects within the lease, and the output and result arrive: no retry.
func TestE2EJobSurvivesReconnect(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, "on: push\njobs:\n  a:\n    runs-on: "+hostOS()+"\n    steps: [{ run: echo before; sleep 1; echo after }]\n")
	eventually(t, "the command to start", func() bool { return strings.Contains(e.jobLog(runID, "a"), "before") })

	for _, s := range e.svc.Sessions().List() {
		e.svc.Sessions().Disconnect(s.WorkerID, context.Canceled)
	}
	run, jobs := e.waitRun(runID)
	if run.State != "succeeded" || jobs["a"].Attempt != 1 {
		t.Fatalf("run %s, attempt %d (%q)", run.State, jobs["a"].Attempt, jobs["a"].Error)
	}
	wantIn(t, e.jobLog(runID, "a"), "before", "after")
}
