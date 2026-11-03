// Package pipeline implements the Opifica pipeline language: a declarative
// YAML file (.opifica/pipeline.yaml) that describes which events start a run
// and which jobs run on which operating systems.
//
// Parse turns the file into a validated Pipeline. Plan turns a Pipeline and a
// concrete Event into the list of jobs to schedule, with matrices expanded,
// conditions evaluated and ${{ }} expressions substituted.
package pipeline

import (
	"fmt"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
)

// DefaultPath is where Opifica looks for the pipeline inside a repository.
const DefaultPath = ".opifica/pipeline.yaml"

// DefaultTimeout applies to jobs that don't set one.
const DefaultTimeout = 60 * time.Minute

// DefaultImage is the golden image used when runs-on doesn't name one.
const DefaultImage = "default"

// Pipeline is a parsed and validated pipeline file.
type Pipeline struct {
	Triggers Triggers
	Env      map[string]string
	// Jobs in the order they appear in the file.
	Jobs []*Job
}

// Triggers decide which events start a run. A nil filter means the event
// type doesn't trigger the pipeline.
type Triggers struct {
	Push        *BranchFilter
	PullRequest *BranchFilter
	Tag         *TagFilter
}

// BranchFilter matches branch names against glob patterns. An empty list
// matches every branch. For pull requests, the target branch is matched.
type BranchFilter struct {
	Branches []string
}

// TagFilter matches tag names against glob patterns. An empty list matches
// every tag.
type TagFilter struct {
	Patterns []string
}

// Job is one job definition, before matrix expansion.
type Job struct {
	ID      string
	Name    string
	RunsOn  RunsOn
	Matrix  Matrix
	Needs   []string
	If      string
	Env     map[string]string
	Secrets []string
	Timeout time.Duration
	Steps   []Step

	ifProgram cel.Program
}

// RunsOn selects the kind of worker and VM image a job needs.
type RunsOn struct {
	OS     string // linux, windows or macos
	Arch   string // amd64, arm64, or empty for any
	Image  string
	Labels map[string]string
}

// Matrix expands one job into one instance per combination of values.
type Matrix struct {
	// Keys in file order, so expansion order is stable.
	Keys   []string
	Values map[string][]string
}

// Empty reports whether the job has no matrix.
func (m Matrix) Empty() bool { return len(m.Keys) == 0 }

// Step is one step of a job. Exactly one of Run, Report, Upload or Sign is set.
type Step struct {
	Name            string
	Run             *RunAction
	Report          *ReportAction
	Upload          *UploadAction
	Sign            *SignAction
	Env             map[string]string
	Secrets         []string
	ContinueOnError bool
	Timeout         time.Duration
}

// RunAction runs a command in the job VM.
type RunAction struct {
	Command          string
	Shell            string
	WorkingDirectory string
}

// ReportAction collects test reports.
type ReportAction struct {
	Format string // currently only "junit"
	Paths  []string
}

// UploadAction stores files as run artifacts.
type UploadAction struct {
	Name  string
	Paths []string
}

// SignAction asks the signing service to sign artifacts. Only their digests
// leave the VM.
type SignAction struct {
	Paths   []string
	Profile string
}

// Kind returns the step's action name, e.g. "run".
func (s Step) Kind() string {
	switch {
	case s.Run != nil:
		return "run"
	case s.Report != nil:
		return "report"
	case s.Upload != nil:
		return "upload"
	case s.Sign != nil:
		return "sign"
	}
	return ""
}

// Error is a problem found in a pipeline file, with its position.
type Error struct {
	Line   int
	Column int
	// Path in the document, e.g. "jobs.test.steps[1].run".
	Path string
	Msg  string
}

func (e *Error) Error() string {
	var b strings.Builder
	switch {
	case e.Line > 0 && e.Column > 0:
		fmt.Fprintf(&b, "%d:%d: ", e.Line, e.Column)
	case e.Line > 0:
		fmt.Fprintf(&b, "%d: ", e.Line)
	}
	if e.Path != "" {
		b.WriteString(e.Path)
		b.WriteString(": ")
	}
	b.WriteString(e.Msg)
	return b.String()
}

// Errors is a list of problems. Parse and Plan return every problem they find
// instead of stopping at the first one.
type Errors []*Error

func (es Errors) Error() string {
	msgs := make([]string, len(es))
	for i, e := range es {
		msgs[i] = e.Error()
	}
	return strings.Join(msgs, "\n")
}
