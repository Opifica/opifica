package integration

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/repos"
	"github.com/opifica/opifica/pkg/worker"
)

var (
	opificaOnce sync.Once
	opificaBin  string
	opificaErr  error
)

// opificaBinary builds the opifica CLI once for the tests that run it.
func opificaBinary(t *testing.T) string {
	t.Helper()
	opificaOnce.Do(func() {
		dir, err := os.MkdirTemp("", "opifica-cli-")
		if err != nil {
			opificaErr = err
			return
		}
		opificaBin = filepath.Join(dir, "opifica")
		out, err := exec.Command("go", "build", "-o", opificaBin, "github.com/opifica/opifica/cmd/opifica").CombinedOutput()
		if err != nil {
			opificaErr = fmt.Errorf("%v: %s", err, out)
		}
	})
	if opificaErr != nil {
		t.Fatalf("building opifica: %v", opificaErr)
	}
	return opificaBin
}

func (e *env) fingerprint() string { return e.svc.CA.Fingerprint() }

// triggerClient is what `opifica trigger` uses, with a given token.
func (e *env) triggerClient(t *testing.T, token string) opificav1connect.TriggerServiceClient {
	t.Helper()
	cfg, err := worker.PinnedCAConfig(e.fingerprint(), "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	auth := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	})
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: true}}
	return opificav1connect.NewTriggerServiceClient(hc, e.url, connect.WithInterceptors(auth))
}

func (e *env) registerRepo(t *testing.T, name, cloneURL string, routes ...string) string {
	t.Helper()
	var parsed []repos.Route
	for _, r := range routes {
		route, err := repos.ParseRoute(r)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, route)
	}
	if _, err := e.repos.Create(context.Background(), name, cloneURL, parsed, nil); err != nil {
		t.Fatal(err)
	}
	token, err := e.repos.CreateToken(context.Background(), name, "test")
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func pipelineFor(body string) string {
	return strings.ReplaceAll(body, "OS", hostOS())
}

const simplePipeline = `on: [push, tag, pull_request]
jobs:
  build:
    runs-on: OS
    steps: [{ run: echo built $OPIFICA_REF }]
`

func TestTriggerAPI(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	repo, sha := gitRepo(t, map[string]string{".opifica/pipeline.yaml": pipelineFor(simplePipeline)})
	token := e.registerRepo(t, "acme/app", repo, "push:main=trusted", "tag:v*=release")
	client := e.triggerClient(t, token)
	ctx := context.Background()

	resp, err := client.Trigger(ctx, connect.NewRequest(&opificav1.TriggerRequest{
		Repository: "acme/app", Event: "push", Ref: "refs/heads/main", CommitSha: sha,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetPool() != "trusted" || resp.Msg.GetJobs() != 1 {
		t.Errorf("trigger = %+v", resp.Msg)
	}
	runID := uuid.MustParse(resp.Msg.GetRunId())
	if run, _ := e.waitRun(runID); run.State != "succeeded" || run.RepositoryID == nil {
		t.Fatalf("run %s, repository %v", run.State, run.RepositoryID)
	}

	got, err := client.GetRun(ctx, connect.NewRequest(&opificav1.GetRunRequest{RunId: runID.String()}))
	if err != nil || got.Msg.GetState() != "succeeded" || len(got.Msg.GetJobs()) != 1 {
		t.Fatalf("GetRun = %+v, %v", got, err)
	}
	lg, err := client.GetJobLog(ctx, connect.NewRequest(&opificav1.GetJobLogRequest{JobId: got.Msg.GetJobs()[0].GetId()}))
	if err != nil || !strings.Contains(string(lg.Msg.GetData()), "built refs/heads/main") || !lg.Msg.GetComplete() {
		t.Errorf("GetJobLog = %q complete=%v, %v", lg.Msg.GetData(), lg.Msg.GetComplete(), err)
	}

	// Nothing to do: no route, or a commit without a pipeline file.
	resp, err = client.Trigger(ctx, connect.NewRequest(&opificav1.TriggerRequest{
		Repository: "acme/app", Event: "push", Ref: "refs/heads/feature", CommitSha: sha,
	}))
	if err != nil || resp.Msg.GetRunId() != "" || !strings.Contains(resp.Msg.GetMessage(), "no routing rule") {
		t.Errorf("unrouted push = %+v, %v", resp.Msg, err)
	}
}

func TestTriggerAuthorization(t *testing.T) {
	e := newEnv(t)
	repoA, shaA := gitRepo(t, map[string]string{".opifica/pipeline.yaml": pipelineFor(simplePipeline)})
	tokenA := e.registerRepo(t, "acme/a", repoA, "*=trusted")
	tokenB := e.registerRepo(t, "acme/b", repoA, "*=trusted")
	ctx := context.Background()
	req := func(repo string) *connect.Request[opificav1.TriggerRequest] {
		return connect.NewRequest(&opificav1.TriggerRequest{Repository: repo, Event: "push", Ref: "refs/heads/main", CommitSha: shaA})
	}

	if _, err := e.triggerClient(t, "opr_made-up").Trigger(ctx, req("acme/a")); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("unknown token: %v", err)
	}
	if _, err := e.triggerClient(t, tokenB).Trigger(ctx, req("acme/a")); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("another repository's token: %v", err)
	}
	resp, err := e.triggerClient(t, tokenA).Trigger(ctx, req("acme/a"))
	if err != nil {
		t.Fatal(err)
	}
	// B's token can't read A's runs or logs either.
	if _, err := e.triggerClient(t, tokenB).GetRun(ctx, connect.NewRequest(&opificav1.GetRunRequest{RunId: resp.Msg.GetRunId()})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("reading another repository's run: %v", err)
	}
	jobs, _ := e.svc.Store.ListRunJobs(ctx, uuid.MustParse(resp.Msg.GetRunId()))
	if _, err := e.triggerClient(t, tokenB).GetJobLog(ctx, connect.NewRequest(&opificav1.GetJobLogRequest{JobId: jobs[0].ID.String()})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("reading another repository's log: %v", err)
	}
	// A revoked token stops working.
	toks, _ := e.svc.Store.ListTriggerTokens(ctx, mustRepoID(t, e, "acme/a"))
	e.svc.Store.RevokeTriggerToken(ctx, toks[0].ID)
	if _, err := e.triggerClient(t, tokenA).Trigger(ctx, req("acme/a")); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("revoked token: %v", err)
	}
}

func mustRepoID(t *testing.T, e *env, name string) uuid.UUID {
	r, err := e.repos.Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return r.ID
}

// A repository's read credential reaches the worker with each job, and is
// never stored in the job.
func TestRepositoryCredentialReachesWorker(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("w", workerOpts{})
	repo, sha := gitRepo(t, map[string]string{".opifica/pipeline.yaml": "on: push\njobs:\n  a:\n    runs-on: linux\n    steps: [{ run: x }]\n"})
	if _, err := e.repos.Create(context.Background(), "acme/private", repo, []repos.Route{{Event: "*", Pool: "trusted"}}, []byte("ghp_read_only_token")); err != nil {
		t.Fatal(err)
	}
	token, _ := e.repos.CreateToken(context.Background(), "acme/private", "")
	resp, err := e.triggerClient(t, token).Trigger(context.Background(), connect.NewRequest(&opificav1.TriggerRequest{
		Repository: "acme/private", Event: "push", Ref: "refs/heads/main", CommitSha: sha,
	}))
	if err != nil {
		t.Fatal(err)
	}
	e.waitRun(uuid.MustParse(resp.Msg.GetRunId()))
	jobs := w.jobs()
	if len(jobs) != 1 || jobs[0].GetSource().GetCredential().GetToken() != "ghp_read_only_token" {
		t.Fatalf("worker got credential %v", jobs)
	}
	stored, _ := e.svc.Store.ListRunJobs(context.Background(), uuid.MustParse(resp.Msg.GetRunId()))
	if strings.Contains(string(stored[0].Spec), "ghp_read_only_token") {
		t.Error("the credential is stored in the job")
	}
	var raw []byte
	e.svc.Store.Pool.QueryRow(context.Background(), "SELECT credential_ciphertext FROM repositories WHERE name = 'acme/private'").Scan(&raw)
	if strings.Contains(string(raw), "ghp_read_only_token") {
		t.Error("the credential is stored unencrypted")
	}
}

// The production path for hosted repositories: a bare repository with the
// post-receive hook, and a developer pushing to it.
func TestGitHookStartsRuns(t *testing.T) {
	e := newEnv(t)
	e.realWorkerIn("trusted", 1)
	e.realWorkerIn("release", 1)
	bin := opificaBinary(t)

	bare := filepath.Join(t.TempDir(), "app.git")
	git(t, "", "init", "-q", "--bare", "-b", "main", bare)
	token := e.registerRepo(t, "acme/app", bare, "push:main=trusted", "tag:v*=release")
	hook := fmt.Sprintf("#!/bin/sh\nexport OPIFICA_CONTROLLER=%q OPIFICA_CA_FINGERPRINT=%q OPIFICA_REPO=acme/app OPIFICA_TRIGGER_TOKEN=%q\nexec %q hook post-receive\n",
		e.url, e.fingerprint(), token, bin)
	if err := os.WriteFile(filepath.Join(bare, "hooks", "post-receive"), []byte(hook), 0o755); err != nil {
		t.Fatal(err)
	}

	work := t.TempDir()
	git(t, work, "clone", "-q", bare, ".")
	os.MkdirAll(filepath.Join(work, ".opifica"), 0o755)
	os.WriteFile(filepath.Join(work, ".opifica", "pipeline.yaml"), []byte(pipelineFor(simplePipeline)), 0o644)
	git(t, work, "add", ".")
	git(t, work, "commit", "-q", "-m", "add pipeline")
	git(t, work, "tag", "v1.0")

	out := git(t, work, "push", "origin", "main", "v1.0")
	runIDs := regexp.MustCompile(`opifica: (main|v1\.0): run (\S+) started in pool (\S+)`).FindAllStringSubmatch(out, -1)
	if len(runIDs) != 2 {
		t.Fatalf("push output doesn't show two runs:\n%s", out)
	}
	pools := map[string]string{}
	for _, m := range runIDs {
		pools[m[1]] = m[3]
		if run, _ := e.waitRun(uuid.MustParse(m[2])); run.State != "succeeded" {
			t.Errorf("run for %s: %s", m[1], run.State)
		}
	}
	if pools["main"] != "trusted" || pools["v1.0"] != "release" {
		t.Errorf("pools = %v; want main→trusted, v1.0→release", pools)
	}

	// A push no route matches tells the pusher, and the push still succeeds.
	git(t, work, "checkout", "-q", "-b", "feature")
	out = git(t, work, "push", "origin", "feature")
	if !strings.Contains(out, "opifica: feature: no routing rule") {
		t.Errorf("unrouted push output:\n%s", out)
	}
}

// The GitHub Actions path: `opifica trigger --wait` in a workflow step, with
// the environment GitHub provides.
func TestTriggerFromGitHubActions(t *testing.T) {
	e := newEnv(t)
	e.realWorker(2)
	bin := opificaBinary(t)
	repo, sha := gitRepo(t, map[string]string{
		".opifica/pipeline.yaml": pipelineFor(`on: push
jobs:
  build:
    runs-on: OS
    steps: [{ run: echo hello from opifica }]
  test:
    needs: build
    runs-on: OS
    steps: [{ run: "if [ -f FAIL ]; then echo failing; exit 4; fi" }]
`)})
	token := e.registerRepo(t, "acme/app", repo, "push:main=trusted")

	run := func(sha string) (string, string, int) {
		summary := filepath.Join(t.TempDir(), "summary.md")
		cmd := exec.Command(bin, "trigger", "-wait", "-timeout", "2m")
		cmd.Env = append(os.Environ(),
			"GITHUB_ACTIONS=true", "GITHUB_EVENT_NAME=push", "GITHUB_REF=refs/heads/main", "GITHUB_SHA="+sha,
			"GITHUB_STEP_SUMMARY="+summary,
			"OPIFICA_CONTROLLER="+e.url, "OPIFICA_CA_FINGERPRINT="+e.fingerprint(),
			"OPIFICA_REPO=acme/app", "OPIFICA_TRIGGER_TOKEN="+token)
		out, _ := cmd.CombinedOutput()
		data, _ := os.ReadFile(summary)
		return string(out), string(data), cmd.ProcessState.ExitCode()
	}

	out, summary, code := run(sha)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	wantIn(t, out, "started in pool trusted with 2 job(s)", "[build] hello from opifica", "succeeded")
	wantIn(t, summary, "| build |", "| test |", ": succeeded")

	// A failing run fails the step.
	os.WriteFile(filepath.Join(repo, "FAIL"), []byte("x"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "break it")
	out, _, code = run(git(t, repo, "rev-parse", "HEAD"))
	if code != 1 || !strings.Contains(out, "[test] failing") || !strings.Contains(out, "(exit 4)") {
		t.Errorf("failing run: exit %d:\n%s", code, out)
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
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
