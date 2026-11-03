package main

import (
	"os"
	"path/filepath"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestGitHubEvent(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	ev, err := githubEvent(env(map[string]string{"GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": sha}))
	if err != nil || ev != (triggerEvent{event: "push", ref: "refs/heads/main", sha: sha}) {
		t.Errorf("push: %+v, %v", ev, err)
	}
	ev, err = githubEvent(env(map[string]string{"GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/tags/v1.0", "GITHUB_SHA": sha}))
	if err != nil || ev.event != "tag" {
		t.Errorf("tag push: %+v, %v", ev, err)
	}

	payload := filepath.Join(t.TempDir(), "event.json")
	os.WriteFile(payload, []byte(`{"pull_request":{"head":{"ref":"fix","sha":"2222222222222222222222222222222222222222"},"base":{"ref":"main"}}}`), 0o644)
	ev, err = githubEvent(env(map[string]string{
		"GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": payload,
		"GITHUB_REF": "refs/pull/7/merge", "GITHUB_SHA": "3333333333333333333333333333333333333333",
	}))
	want := triggerEvent{event: "pull_request", ref: "refs/heads/fix", sha: "2222222222222222222222222222222222222222", base: "main"}
	if err != nil || ev != want {
		t.Errorf("pull request: %+v, %v; want the head commit, not GitHub's merge commit", ev, err)
	}

	if _, err := githubEvent(env(map[string]string{"GITHUB_EVENT_NAME": "schedule"})); err == nil {
		t.Error("unsupported event accepted")
	}
}
