package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"github.com/opifica/opifica/internal/controller/logs"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/pipeline"
)

func runCommand(sub string, args []string, stdout, stderr io.Writer) int {
	switch sub {
	case "create":
		return runCreate(args, stdout, stderr)
	case "list":
		return runList(args, stdout, stderr)
	case "show":
		return runShow(args, stdout, stderr)
	case "cancel":
		return runCancel(args, stdout, stderr)
	case "logs":
		return runLogs(args, stdout, stderr)
	}
	fmt.Fprintf(stderr, "controller: unknown command \"run %s\"\n\n%s", sub, usage)
	return 2
}

// withScheduler opens the store and builds a scheduler without running its
// loop: the serving controller picks up what this process writes.
func withScheduler(c common, stderr io.Writer, fn func(ctx context.Context, sched *scheduler.Scheduler, q *db.Queries) error) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	svc, closeFn, err := c.open(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	defer closeFn()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	sched := scheduler.New(svc.Store, svc.Sessions(), quiet, scheduler.Config{})
	if err := fn(ctx, sched, svc.Store.Queries); err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	return 0
}

func runCreate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	repo := fs.String("repo", "", "repository name, e.g. acme/app (required)")
	cloneURL := fs.String("clone-url", "", "URL the worker clones from")
	file := fs.String("file", pipeline.DefaultPath, "pipeline file")
	pool := fs.String("pool", "", "pool to run in (required); later decided by routing policy")
	event := fs.String("event", "push", "push, pull_request or tag")
	branch := fs.String("branch", "main", "branch (push) or source branch (pull request)")
	base := fs.String("base", "main", "pull request target branch")
	tag := fs.String("tag", "", "tag name, for -event tag")
	sha := fs.String("sha", "", "commit SHA (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *repo == "" || *pool == "" || *sha == "" {
		fmt.Fprintln(stderr, "controller: -repo, -pool and -sha are required")
		return 2
	}
	src, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	ev := pipeline.Event{Type: pipeline.EventType(*event), SHA: *sha}
	switch ev.Type {
	case pipeline.EventPush:
		ev.Branch, ev.Ref = *branch, "refs/heads/"+*branch
	case pipeline.EventPullRequest:
		ev.Branch, ev.BaseBranch = *branch, *base
	case pipeline.EventTag:
		ev.Tag, ev.Ref = *tag, "refs/tags/"+*tag
	default:
		fmt.Fprintf(stderr, "controller: unknown event %q\n", *event)
		return 2
	}
	return withScheduler(c, stderr, func(ctx context.Context, sched *scheduler.Scheduler, q *db.Queries) error {
		run, err := sched.CreateRun(ctx, scheduler.RunRequest{
			Repo: *repo, CloneURL: *cloneURL, Event: ev, Pool: *pool, Pipeline: src,
		})
		if err != nil {
			return err
		}
		jobs, err := q.ListRunJobs(ctx, run.ID)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Run %s created with %d job(s).\n", run.ID, len(jobs))
		return nil
	})
}

func runList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	limit := fs.Int("n", 20, "number of runs to show")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return withScheduler(c, stderr, func(ctx context.Context, _ *scheduler.Scheduler, q *db.Queries) error {
		runs, err := q.ListRuns(ctx, int32(*limit))
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tREPO\tEVENT\tREF\tCOMMIT\tPOOL\tSTATE\tCREATED")
		for _, r := range runs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.Repo, r.EventType, refName(r),
				short(r.CommitSha), r.Pool, r.State, r.CreatedAt.Local().Format(time.DateTime))
		}
		return tw.Flush()
	})
}

func runShow(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run show", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	id, ok := runIDArg(fs, args, stderr, "show")
	if !ok {
		return 2
	}
	return withScheduler(c, stderr, func(ctx context.Context, _ *scheduler.Scheduler, q *db.Queries) error {
		r, err := q.GetRun(ctx, id)
		if err != nil {
			return fmt.Errorf("run %s: %w", id, err)
		}
		fmt.Fprintf(stdout, "Run %s\n  %s %s @ %s in pool %s: %s\n\n", r.ID, r.Repo, refName(r), short(r.CommitSha), r.Pool, r.State)
		jobs, err := q.ListRunJobs(ctx, id)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "JOB\tOS\tSTATE\tATTEMPT\tWORKER\tEXIT\tNOTE")
		for _, j := range jobs {
			worker, exit := "-", "-"
			if j.WorkerID != nil {
				worker = j.WorkerID.String()[:8]
			}
			if j.ExitCode != nil {
				exit = fmt.Sprint(*j.ExitCode)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%s\t%s\t%s\n", j.Name, j.Os, j.State, j.Attempt, j.MaxAttempts, worker, exit, j.Error)
		}
		return tw.Flush()
	})
}

func runCancel(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run cancel", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	id, ok := runIDArg(fs, args, stderr, "cancel")
	if !ok {
		return 2
	}
	return withScheduler(c, stderr, func(ctx context.Context, sched *scheduler.Scheduler, _ *db.Queries) error {
		if err := sched.CancelRun(ctx, id); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Run %s is being cancelled; running jobs are stopped on their workers.\n", id)
		return nil
	})
}

func runIDArg(fs *flag.FlagSet, args []string, stderr io.Writer, cmd string) (uuid.UUID, bool) {
	pos, err := parseWithArgs(fs, args)
	if err != nil {
		return uuid.UUID{}, false
	}
	if len(pos) != 1 {
		fmt.Fprintf(stderr, "Usage: controller run %s [options] <run-id>\n", cmd)
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(pos[0])
	if err != nil {
		fmt.Fprintf(stderr, "controller: invalid run id %q\n", pos[0])
		return uuid.UUID{}, false
	}
	return id, true
}

// parseWithArgs parses flags that may come before or after positional
// arguments, e.g. both "show -data-dir x ID" and "show ID -data-dir x".
func parseWithArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func refName(r db.Run) string {
	switch {
	case r.Tag != "":
		return r.Tag
	case r.EventType == "pull_request":
		return r.Branch + " -> " + r.BaseBranch
	}
	return strings.TrimPrefix(r.Branch, "refs/heads/")
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func runLogs(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	only := fs.String("job", "", "show only this job (its key, e.g. test[os=linux], or its name)")
	id, ok := runIDArg(fs, args, stderr, "logs")
	if !ok {
		return 2
	}
	store := &logs.Store{Dir: filepath.Join(c.dataDir, "logs")}
	return withScheduler(c, stderr, func(ctx context.Context, _ *scheduler.Scheduler, q *db.Queries) error {
		jobs, err := q.ListRunJobs(ctx, id)
		if err != nil {
			return err
		}
		shown := 0
		for _, j := range jobs {
			if *only != "" && j.Key != *only && j.Name != *only {
				continue
			}
			shown++
			fmt.Fprintf(stdout, "---- %s (%s, %s) ----\n", j.Name, j.Os, j.State)
			r, err := store.Open(j.ID)
			if errors.Is(err, logs.ErrNoLog) {
				fmt.Fprintln(stdout, "(no output)")
				continue
			}
			if err != nil {
				return err
			}
			_, err = io.Copy(stdout, r)
			r.Close()
			if err != nil {
				return err
			}
		}
		if *only != "" && shown == 0 {
			return fmt.Errorf("run %s has no job %q", id, *only)
		}
		return nil
	})
}
