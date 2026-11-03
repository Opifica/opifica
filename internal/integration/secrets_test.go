package integration

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/secrets"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/pipeline"
)

const secretValue = "s3cr3t-deploy-token-value"

func (e *env) setSecret(name string, p secrets.Policy) {
	e.t.Helper()
	if err := e.secrets.Set(context.Background(), name, []byte(secretValue), p); err != nil {
		e.t.Fatal(err)
	}
}

const deployPipeline = `
on: [push, pull_request, tag]
jobs:
  deploy:
    runs-on: OS
    secrets: [DEPLOY_TOKEN]
    steps:
      - run: |
          echo "token is $DEPLOY_TOKEN"
          # Check the real value arrived, without the pipeline containing it.
          test "${#DEPLOY_TOKEN}" = 25
          case "$DEPLOY_TOKEN" in s3cr3t-deploy-*) ;; *) exit 1 ;; esac
        secrets: [DEPLOY_TOKEN]
      - name: only the step that asked gets it
        run: test -z "$DEPLOY_TOKEN"
`

func TestE2ESecretGrantedAndMasked(t *testing.T) {
	e := newEnv(t)
	e.realWorker(1)
	e.setSecret("DEPLOY_TOKEN", secrets.Policy{Repos: []string{"acme/*"}, Pools: []string{"trusted"}, Refs: []string{"refs/heads/main"}})
	repo, sha := gitRepo(t, map[string]string{"README": "x"})
	runID := e.startRun(repo, sha, strings.ReplaceAll(deployPipeline, "OS", hostOS()))

	run, jobs := e.waitRun(runID)
	log := e.jobLog(runID, "deploy")
	if run.State != "succeeded" {
		t.Fatalf("run %s (%q)\n%s", run.State, jobs["deploy"].Error, log)
	}
	wantIn(t, log, "token is ***")
	if strings.Contains(log, secretValue) {
		t.Errorf("the secret value is in the stored log:\n%s", log)
	}

	acc, err := e.svc.Store.ListSecretAccesses(context.Background(), dbAccessParams("DEPLOY_TOKEN"))
	if err != nil || len(acc) != 1 || !acc[0].Granted || *acc[0].JobID != jobs["deploy"].ID {
		t.Errorf("audit = %+v, %v; want one granted access for the job", acc, err)
	}
}

// The admin's policy decides, whatever the pipeline asks for.
func TestE2ESecretPolicyDenials(t *testing.T) {
	tests := []struct {
		name   string
		policy secrets.Policy
		event  pipeline.Event
		want   string
	}{
		{"other pool", secrets.Policy{Repos: []string{"*"}, Pools: []string{"release"}},
			pipeline.Event{Type: pipeline.EventPush, Branch: "main", Ref: "refs/heads/main"}, "not allowed in pool trusted"},
		{"other repository", secrets.Policy{Repos: []string{"other/*"}, Pools: []string{"trusted"}},
			pipeline.Event{Type: pipeline.EventPush, Branch: "main", Ref: "refs/heads/main"}, "not allowed for repository acme/app"},
		{"other branch", secrets.Policy{Repos: []string{"*"}, Pools: []string{"trusted"}, Refs: []string{"refs/heads/main"}},
			pipeline.Event{Type: pipeline.EventPush, Branch: "feature", Ref: "refs/heads/feature"}, "not allowed for refs/heads/feature"},
		{"pull request", secrets.Policy{Repos: []string{"*"}, Pools: []string{"trusted"}},
			pipeline.Event{Type: pipeline.EventPullRequest, Branch: "evil", BaseBranch: "main"}, "not available to pull requests"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.realWorker(1)
			e.setSecret("DEPLOY_TOKEN", tt.policy)
			repo, sha := gitRepo(t, map[string]string{"README": "x"})
			ev := tt.event
			ev.SHA = sha
			run, err := e.sched.CreateRun(context.Background(), scheduler.RunRequest{
				Repo: "acme/app", CloneURL: repo, Pool: "trusted", Event: ev,
				Pipeline: []byte(strings.ReplaceAll(deployPipeline, "OS", hostOS())),
			})
			if err != nil {
				t.Fatal(err)
			}
			got, jobs := e.waitRun(run.ID)
			j := jobs["deploy"]
			if got.State != "failed" || !strings.Contains(j.Error, tt.want) || j.Attempt != 1 {
				t.Errorf("run %s, attempt %d, error %q; want failed with %q", got.State, j.Attempt, j.Error, tt.want)
			}
			// The step's output, not its "==> Step" header line.
			if regexp.MustCompile(`(?m)^token is`).MatchString(e.jobLog(run.ID, "deploy")) {
				t.Error("the step ran without its secret")
			}
		})
	}
}

// Requests that try to get around the checks, sent straight over a
// worker's connection.
func TestSecretRequestChecks(t *testing.T) {
	e := newEnv(t)
	e.setSecret("DEPLOY_TOKEN", secrets.Policy{Repos: []string{"*"}, Pools: []string{"trusted"}})
	e.setSecret("OTHER_TOKEN", secrets.Policy{Repos: []string{"*"}, Pools: []string{"trusted"}})

	holder := e.fakeWorker("holder", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{hang: true} }})
	e.createRun("on: push\njobs:\n  deploy:\n    runs-on: linux\n    secrets: [DEPLOY_TOKEN]\n    steps: [{ run: x }]\n", pipeline.Event{}, "")
	eventually(t, "the job to reach the worker", func() bool { return len(holder.jobs()) == 1 })
	job := holder.jobs()[0]
	eventually(t, "the job to be running", func() bool {
		j, err := e.svc.Store.GetJob(context.Background(), uuid.MustParse(job.GetId()))
		return err == nil && j.State == "running"
	})

	ask := func(t *testing.T, fw *fakeWorker, jobID, token, name string) *opificav1.SecretResponse {
		t.Helper()
		fw.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_SecretRequest{SecretRequest: &opificav1.SecretRequest{
			RequestId: uuid.NewString(), JobId: jobID, JobToken: token, Name: name,
		}}})
		select {
		case r := <-fw.secretResp:
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("no answer to the secret request")
			return nil
		}
	}

	if r := ask(t, holder, job.GetId(), job.GetJobToken(), "DEPLOY_TOKEN"); string(r.GetValue()) != secretValue {
		t.Fatalf("legitimate request denied: %q", r.GetDeniedReason())
	}

	other := e.fakeWorker("other", workerOpts{})
	select {
	case <-other.Connected():
	case <-time.After(5 * time.Second):
		t.Fatal("second worker did not connect")
	}
	for _, tt := range []struct {
		name               string
		fw                 *fakeWorker
		jobID, token, want string
	}{
		{"secret the job doesn't list", holder, job.GetId(), job.GetJobToken(), "don't include OTHER_TOKEN"},
		{"wrong job token", holder, job.GetId(), "opt_guessed", "invalid job token"},
		{"another worker's job", other, job.GetId(), job.GetJobToken(), "not running on this worker"},
		{"unknown job", holder, uuid.NewString(), job.GetJobToken(), "not running on this worker"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			name := "DEPLOY_TOKEN"
			if strings.Contains(tt.name, "doesn't list") {
				name = "OTHER_TOKEN"
			}
			r := ask(t, tt.fw, tt.jobID, tt.token, name)
			if r.GetValue() != nil || !strings.Contains(r.GetDeniedReason(), tt.want) {
				t.Errorf("got value %q, denied %q; want denial %q", r.GetValue(), r.GetDeniedReason(), tt.want)
			}
		})
	}

	// Every request was recorded, granted or not.
	acc, err := e.svc.Store.ListSecretAccesses(context.Background(), dbAccessParams("DEPLOY_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	granted := 0
	for _, a := range acc {
		if a.Granted {
			granted++
		}
	}
	if len(acc) != 4 || granted != 1 {
		t.Errorf("%d DEPLOY_TOKEN accesses recorded (%d granted), want 4 (1 granted)", len(acc), granted)
	}
}

func dbAccessParams(name string) db.ListSecretAccessesParams {
	return db.ListSecretAccessesParams{SecretName: name, Limit: 100}
}
