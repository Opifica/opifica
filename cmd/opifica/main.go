// Command opifica is the Opifica CLI for developers and admins.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/opifica/opifica/internal/pipeline"
	"github.com/opifica/opifica/internal/version"
	"github.com/opifica/opifica/schema"
)

const usage = `Usage: opifica <command> [arguments]

Commands:
  validate [file]   Check a pipeline file (default: .opifica/pipeline.yaml)
  plan [file]       Show which jobs an event would run
  schema            Print the JSON Schema for pipeline files
  trigger           Start a run on the controller (e.g. from GitHub Actions)
  hook post-receive Git hook for hosted repositories: start runs for pushes
  version           Print the version

Run "opifica <command> -h" for a command's options.
`

func main() {
	os.Exit(runWithInput(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithInput(args, strings.NewReader(""), stdout, stderr)
}

func runWithInput(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "validate":
		return validate(args[1:], stdout, stderr)
	case "plan":
		return plan(args[1:], stdout, stderr)
	case "trigger":
		return trigger(args[1:], stdout, stderr)
	case "hook":
		return hook(args[1:], stdin, stdout)
	case "schema":
		stdout.Write(schema.Pipeline)
		return 0
	case "version":
		fmt.Fprintln(stdout, version.Version)
		return 0
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	}
	fmt.Fprintf(stderr, "opifica: unknown command %q\n\n%s", args[0], usage)
	return 2
}

func load(file string, stderr io.Writer) (*pipeline.Pipeline, bool) {
	data, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(stderr, "opifica: %v\n", err)
		return nil, false
	}
	p, err := pipeline.Parse(data)
	if err != nil {
		printErrors(stderr, file, err)
		return nil, false
	}
	return p, true
}

func printErrors(w io.Writer, file string, err error) {
	var errs pipeline.Errors
	if !errors.As(err, &errs) {
		fmt.Fprintf(w, "%s: %v\n", file, err)
		return
	}
	for _, e := range errs {
		fmt.Fprintf(w, "%s:%v\n", file, e)
	}
	fmt.Fprintf(w, "%d problem(s) found\n", len(errs))
}

func fileArg(fs *flag.FlagSet) string {
	if fs.NArg() > 0 {
		return fs.Arg(0)
	}
	return pipeline.DefaultPath
}

func validate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintln(stderr, "Usage: opifica validate [file]") }
	if err := fs.Parse(args); err != nil {
		return 2
	}
	file := fileArg(fs)
	p, ok := load(file, stderr)
	if !ok {
		return 1
	}
	fmt.Fprintf(stdout, "%s: OK (%d jobs)\n", file, len(p.Jobs))
	return 0
}

func plan(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	event := fs.String("event", "push", "event type: push, pull_request or tag")
	branch := fs.String("branch", "main", "pushed branch, or the pull request's source branch")
	base := fs.String("base", "main", "pull request target branch")
	tag := fs.String("tag", "", "tag name (for -event tag)")
	sha := fs.String("sha", "0000000000000000000000000000000000000000", "commit SHA")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage: opifica plan [options] [file]\n\nOptions:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ev := pipeline.Event{Type: pipeline.EventType(*event), SHA: *sha}
	switch ev.Type {
	case pipeline.EventPush:
		ev.Branch, ev.Ref = *branch, "refs/heads/"+*branch
	case pipeline.EventPullRequest:
		ev.Branch, ev.BaseBranch = *branch, *base
	case pipeline.EventTag:
		if *tag == "" {
			fmt.Fprintln(stderr, "opifica: -event tag needs -tag")
			return 2
		}
		ev.Tag, ev.Ref = *tag, "refs/tags/"+*tag
	default:
		fmt.Fprintf(stderr, "opifica: unknown event %q (push, pull_request or tag)\n", *event)
		return 2
	}

	file := fileArg(fs)
	p, ok := load(file, stderr)
	if !ok {
		return 1
	}
	result, err := p.Plan(ev)
	if err != nil {
		printErrors(stderr, file, err)
		return 1
	}
	if !p.Matches(ev) {
		fmt.Fprintf(stdout, "%s does not trigger this pipeline\n", describe(ev))
		return 0
	}

	fmt.Fprintf(stdout, "%s: %d job(s) to run, %d skipped\n\n", describe(ev), len(result.Jobs), len(result.Skipped))
	// Show needs by job, not by matrix instance: "test", not six "test[...]".
	jobOf := map[string]string{}
	for _, j := range result.Jobs {
		jobOf[j.ID] = j.JobID
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tOS\tARCH\tIMAGE\tSTEPS\tNEEDS")
	for _, j := range result.Jobs {
		arch := j.RunsOn.Arch
		if arch == "" {
			arch = "any"
		}
		var needJobs []string
		for _, n := range j.Needs {
			if !slices.Contains(needJobs, jobOf[n]) {
				needJobs = append(needJobs, jobOf[n])
			}
		}
		needs := strings.Join(needJobs, ", ")
		if needs == "" {
			needs = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n", j.Name, j.RunsOn.OS, arch, j.RunsOn.Image, len(j.Steps), needs)
	}
	tw.Flush()
	if len(result.Skipped) > 0 {
		fmt.Fprintf(stdout, "\nSkipped: %s\n", strings.Join(result.Skipped, ", "))
	}
	return 0
}

func describe(ev pipeline.Event) string {
	switch ev.Type {
	case pipeline.EventPullRequest:
		return fmt.Sprintf("Pull request %s -> %s", ev.Branch, ev.BaseBranch)
	case pipeline.EventTag:
		return "Tag " + ev.Tag
	}
	return "Push to " + ev.Branch
}
