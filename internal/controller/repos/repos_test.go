package repos

import (
	"strings"
	"testing"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/pipeline"
)

func TestParseRoute(t *testing.T) {
	for in, want := range map[string]Route{
		"push:main=trusted":       {Event: "push", Ref: "main", Pool: "trusted"},
		"tag:v*=release":          {Event: "tag", Ref: "v*", Pool: "release"},
		"pull_request=untrusted":  {Event: "pull_request", Pool: "untrusted"},
		"*=untrusted":             {Event: "*", Pool: "untrusted"},
		"push:release/**=trusted": {Event: "push", Ref: "release/**", Pool: "trusted"},
	} {
		got, err := ParseRoute(in)
		if err != nil || got != want {
			t.Errorf("ParseRoute(%q) = %+v, %v; want %+v", in, got, err, want)
		}
		if got.String() != in {
			t.Errorf("String() = %q, want %q", got.String(), in)
		}
	}
	for _, bad := range []string{"", "push", "merge=trusted", "push:main=Trusted", "push:[=trusted", "push:main="} {
		if _, err := ParseRoute(bad); err == nil {
			t.Errorf("ParseRoute(%q) accepted", bad)
		}
	}
}

func TestPool(t *testing.T) {
	routes := []Route{
		{Event: "push", Ref: "main", Pool: "trusted"},
		{Event: "push", Ref: "release/**", Pool: "trusted"},
		{Event: "tag", Ref: "v*", Pool: "release"},
		{Event: "pull_request", Ref: "main", Pool: "untrusted"},
	}
	tests := []struct {
		ev   pipeline.Event
		want string
		ok   bool
	}{
		{pipeline.Event{Type: pipeline.EventPush, Branch: "main"}, "trusted", true},
		{pipeline.Event{Type: pipeline.EventPush, Branch: "release/1.0"}, "trusted", true},
		{pipeline.Event{Type: pipeline.EventPush, Branch: "feature"}, "", false},
		{pipeline.Event{Type: pipeline.EventTag, Tag: "v2.0"}, "release", true},
		{pipeline.Event{Type: pipeline.EventTag, Tag: "nightly"}, "", false},
		// For pull requests the target branch counts, never the source:
		// naming your branch "main" gets you nothing.
		{pipeline.Event{Type: pipeline.EventPullRequest, Branch: "main", BaseBranch: "develop"}, "", false},
		{pipeline.Event{Type: pipeline.EventPullRequest, Branch: "feature", BaseBranch: "main"}, "untrusted", true},
	}
	for _, tt := range tests {
		got, ok := Pool(routes, tt.ev)
		if got != tt.want || ok != tt.ok {
			t.Errorf("Pool(%+v) = %q, %v; want %q, %v", tt.ev, got, ok, tt.want, tt.ok)
		}
	}
	if got, _ := Pool(DefaultRoutes, pipeline.Event{Type: pipeline.EventTag, Tag: "v1"}); got != "untrusted" {
		t.Errorf("default routes send a tag to %q", got)
	}
}

func TestEvent(t *testing.T) {
	sha := strings.Repeat("a", 40)
	ev, err := event(&opificav1.TriggerRequest{Event: "push", Ref: "refs/heads/main", CommitSha: sha})
	if err != nil || ev.Type != pipeline.EventPush || ev.Branch != "main" {
		t.Errorf("push: %+v, %v", ev, err)
	}
	ev, err = event(&opificav1.TriggerRequest{Event: "tag", Ref: "refs/tags/v1", CommitSha: sha})
	if err != nil || ev.Tag != "v1" {
		t.Errorf("tag: %+v, %v", ev, err)
	}
	ev, err = event(&opificav1.TriggerRequest{Event: "pull_request", Ref: "refs/heads/fix", CommitSha: sha, BaseBranch: "main"})
	if err != nil || ev.Branch != "fix" || ev.BaseBranch != "main" {
		t.Errorf("pull request: %+v, %v", ev, err)
	}
	for name, req := range map[string]*opificav1.TriggerRequest{
		"short sha":       {Event: "push", Ref: "refs/heads/main", CommitSha: "abc"},
		"tag as push":     {Event: "push", Ref: "refs/tags/v1", CommitSha: sha},
		"branch as tag":   {Event: "tag", Ref: "refs/heads/main", CommitSha: sha},
		"pr without base": {Event: "pull_request", Ref: "refs/heads/x", CommitSha: sha},
		"unknown event":   {Event: "merge", Ref: "refs/heads/main", CommitSha: sha},
	} {
		if _, err := event(req); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
