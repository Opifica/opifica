package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
)

// Exit codes of `opifica trigger`.
const (
	exitRunFailed = 1
	exitUsage     = 2
	exitError     = 3
)

func trigger(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("trigger", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c controllerConfig
	c.fromEnv()
	fs.StringVar(&c.url, "controller", c.url, "controller URL (OPIFICA_CONTROLLER)")
	fs.StringVar(&c.fingerprint, "ca-fingerprint", c.fingerprint, "controller CA fingerprint (OPIFICA_CA_FINGERPRINT)")
	fs.StringVar(&c.repo, "repo", c.repo, "registered repository, e.g. acme/app (OPIFICA_REPO)")
	var ev triggerEvent
	fs.StringVar(&ev.event, "event", "", "push, tag or pull_request (default: from GitHub Actions, or push of the current branch)")
	fs.StringVar(&ev.ref, "ref", "", "full ref, e.g. refs/heads/main")
	fs.StringVar(&ev.sha, "sha", "", "commit to build")
	fs.StringVar(&ev.base, "base", "", "pull request target branch")
	wait := fs.Bool("wait", false, "wait for the run, print its output, and exit non-zero if it fails")
	timeout := fs.Duration("timeout", 6*time.Hour, "with -wait: give up after this long")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Usage: opifica trigger [options]

Starts a run for a commit of a registered repository. In GitHub Actions the
event, ref and commit are read from the workflow's environment; elsewhere
they default to the current branch and commit. The trigger token is read
from OPIFICA_TRIGGER_TOKEN.

Options:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if ev.event == "" && ev.sha == "" {
		var err error
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			ev, err = githubEvent(os.Getenv)
		} else {
			ev, err = localEvent()
		}
		if err != nil {
			fmt.Fprintf(stderr, "opifica: %v\n", err)
			return exitUsage
		}
	}
	client, err := c.client()
	if err != nil {
		fmt.Fprintf(stderr, "opifica: %v\n", err)
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	resp, err := client.Trigger(ctx, connect.NewRequest(&opificav1.TriggerRequest{
		Repository: c.repo, Event: ev.event, Ref: ev.ref, CommitSha: ev.sha, BaseBranch: ev.base,
	}))
	if err != nil {
		fmt.Fprintf(stderr, "opifica: %v\n", err)
		return exitError
	}
	if resp.Msg.GetRunId() == "" {
		fmt.Fprintf(stdout, "No run started: %s\n", resp.Msg.GetMessage())
		return 0
	}
	fmt.Fprintf(stdout, "Run %s started in pool %s with %d job(s).\n", resp.Msg.GetRunId(), resp.Msg.GetPool(), resp.Msg.GetJobs())
	if !*wait {
		return 0
	}
	return follow(ctx, client, resp.Msg.GetRunId(), stdout, stderr)
}

// localEvent is a push of the current branch at HEAD.
func localEvent() (triggerEvent, error) {
	sha, err := gitOutput("rev-parse", "HEAD")
	if err != nil {
		return triggerEvent{}, fmt.Errorf("finding the current commit: %w", err)
	}
	branch, err := gitOutput("symbolic-ref", "--short", "HEAD")
	if err != nil {
		return triggerEvent{}, fmt.Errorf("not on a branch; pass -event, -ref and -sha")
	}
	return triggerEvent{event: "push", ref: "refs/heads/" + branch, sha: sha}, nil
}

func gitOutput(args ...string) (string, error) {
	out, err := exec.Command("git", args...).Output()
	return strings.TrimSpace(string(out)), err
}

// follow prints the run's output as it arrives and returns once it ends.
func follow(ctx context.Context, client opificav1connect.TriggerServiceClient, runID string, stdout, stderr io.Writer) int {
	offsets := map[string]int64{}
	done := map[string]bool{}
	var last *opificav1.GetRunResponse
	for {
		run, err := client.GetRun(ctx, connect.NewRequest(&opificav1.GetRunRequest{RunId: runID}))
		if err != nil {
			fmt.Fprintf(stderr, "opifica: %v\n", err)
			return exitError
		}
		last = run.Msg
		for _, j := range last.GetJobs() {
			if done[j.GetId()] {
				continue
			}
			for {
				lg, err := client.GetJobLog(ctx, connect.NewRequest(&opificav1.GetJobLogRequest{JobId: j.GetId(), Offset: offsets[j.GetId()]}))
				if err != nil {
					fmt.Fprintf(stderr, "opifica: %v\n", err)
					return exitError
				}
				printPrefixed(stdout, j.GetName(), lg.Msg.GetData())
				offsets[j.GetId()] = lg.Msg.GetNextOffset()
				if lg.Msg.GetComplete() {
					done[j.GetId()] = true
				}
				if len(lg.Msg.GetData()) == 0 || lg.Msg.GetComplete() {
					break
				}
			}
		}
		if last.GetState() != "running" && allDone(last, done) {
			break
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			fmt.Fprintln(stderr, "opifica: gave up waiting for the run")
			return exitError
		}
	}

	fmt.Fprintf(stdout, "\nRun %s: %s\n", runID, last.GetState())
	for _, j := range last.GetJobs() {
		line := fmt.Sprintf("  %-10s %s", j.GetState(), j.GetName())
		if j.ExitCode != nil && j.GetExitCode() != 0 {
			line += fmt.Sprintf(" (exit %d)", j.GetExitCode())
		}
		if j.GetError() != "" {
			line += ": " + j.GetError()
		}
		fmt.Fprintln(stdout, line)
	}
	writeStepSummary(runID, last)
	if last.GetState() != "succeeded" {
		return exitRunFailed
	}
	return 0
}

func allDone(run *opificav1.GetRunResponse, done map[string]bool) bool {
	for _, j := range run.GetJobs() {
		if !done[j.GetId()] {
			return false
		}
	}
	return true
}

// printPrefixed prints log data with the job's name before each line.
func printPrefixed(w io.Writer, job string, data []byte) {
	if len(data) == 0 {
		return
	}
	for _, line := range strings.SplitAfter(string(data), "\n") {
		if line != "" {
			fmt.Fprintf(w, "[%s] %s", job, line)
		}
	}
	if data[len(data)-1] != '\n' {
		fmt.Fprintln(w)
	}
}

// writeStepSummary adds a results table to the GitHub Actions run page.
func writeStepSummary(runID string, run *opificav1.GetRunResponse) {
	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "### Opifica run `%s`: %s\n\n| Job | OS | Result |\n| --- | --- | --- |\n", runID, run.GetState())
	for _, j := range run.GetJobs() {
		result := j.GetState()
		if j.GetError() != "" {
			result += ": " + j.GetError()
		}
		fmt.Fprintf(f, "| %s | %s | %s |\n", j.GetName(), j.GetOs(), strings.ReplaceAll(result, "|", "\\|"))
	}
}
