package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// triggerEvent is what to report to the controller.
type triggerEvent struct {
	event, ref, sha, base string
}

// githubEvent reads the event of the GitHub Actions run we are in.
func githubEvent(getenv func(string) string) (triggerEvent, error) {
	name, ref, sha := getenv("GITHUB_EVENT_NAME"), getenv("GITHUB_REF"), getenv("GITHUB_SHA")
	switch name {
	case "push", "workflow_dispatch":
		switch {
		case strings.HasPrefix(ref, "refs/heads/"):
			return triggerEvent{event: "push", ref: ref, sha: sha}, nil
		case strings.HasPrefix(ref, "refs/tags/"):
			return triggerEvent{event: "tag", ref: ref, sha: sha}, nil
		}
		return triggerEvent{}, fmt.Errorf("GITHUB_REF %q is neither a branch nor a tag", ref)
	case "pull_request", "pull_request_target":
		// GITHUB_SHA is GitHub's temporary merge commit; build the head
		// commit the author pushed instead.
		data, err := os.ReadFile(getenv("GITHUB_EVENT_PATH"))
		if err != nil {
			return triggerEvent{}, fmt.Errorf("reading the pull request event: %w", err)
		}
		var payload struct {
			PullRequest struct {
				Head struct{ Ref, Sha string }
				Base struct{ Ref string }
			} `json:"pull_request"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return triggerEvent{}, fmt.Errorf("reading the pull request event: %w", err)
		}
		pr := payload.PullRequest
		if pr.Head.Sha == "" || pr.Base.Ref == "" {
			return triggerEvent{}, fmt.Errorf("the pull request event has no head commit or base branch")
		}
		return triggerEvent{event: "pull_request", ref: "refs/heads/" + pr.Head.Ref, sha: pr.Head.Sha, base: pr.Base.Ref}, nil
	}
	return triggerEvent{}, fmt.Errorf("GitHub event %q is not supported (push, workflow_dispatch, pull_request)", name)
}
