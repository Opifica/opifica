package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func mustParse(t *testing.T, src string) *Pipeline {
	t.Helper()
	p, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse failed:\n%v", err)
	}
	return p
}

// parseErrors parses src, expects it to fail, and returns the error strings.
func parseErrors(t *testing.T, src string) []string {
	t.Helper()
	_, err := Parse([]byte(src))
	if err == nil {
		t.Fatal("Parse succeeded, want errors")
	}
	var es Errors
	if !errors.As(err, &es) {
		t.Fatalf("error is %T, want Errors", err)
	}
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Error()
	}
	return out
}

func wantError(t *testing.T, errs []string, substr string) {
	t.Helper()
	for _, e := range errs {
		if strings.Contains(e, substr) {
			return
		}
	}
	t.Errorf("no error contains %q; got:\n%s", substr, strings.Join(errs, "\n"))
}

func TestExamplesParse(t *testing.T) {
	files, err := filepath.Glob("testdata/valid/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Parse(data); err != nil {
				t.Fatalf("Parse failed:\n%v", err)
			}
		})
	}
}

func TestParseFull(t *testing.T) {
	p := mustParse(t, `
on:
  push: { branches: [main, "release/*"] }
  pull_request:
  tag: { pattern: "v*" }
env:
  GOFLAGS: -mod=readonly
jobs:
  test:
    matrix:
      os: [linux, windows, macos]
      go: ["1.26", "1.27"]
    runs-on:
      os: ${{ matrix.os }}
    steps:
      - run: go test ./...
      - report: junit build/*.xml
  release:
    needs: test
    if: event.tag != ""
    runs-on: { os: macos, arch: arm64, image: xcode-16 }
    secrets: [NOTARY_TOKEN]
    timeout: 30m
    steps:
      - name: Build
        run: make dist
        secrets: [NOTARY_TOKEN]
      - upload: { name: dist, paths: [dist/*.dmg] }
      - sign: dist/*.dmg
`)
	if p.Triggers.Push == nil || !slices.Equal(p.Triggers.Push.Branches, []string{"main", "release/*"}) {
		t.Errorf("push trigger = %+v", p.Triggers.Push)
	}
	if p.Triggers.PullRequest == nil {
		t.Error("pull_request with no value should still enable the trigger")
	}
	if p.Triggers.Tag == nil || !slices.Equal(p.Triggers.Tag.Patterns, []string{"v*"}) {
		t.Errorf("tag trigger = %+v", p.Triggers.Tag)
	}
	if len(p.Jobs) != 2 || p.Jobs[0].ID != "test" || p.Jobs[1].ID != "release" {
		t.Fatalf("jobs not in file order: %v", jobIDs(p))
	}

	test := p.Jobs[0]
	if !slices.Equal(test.Matrix.Keys, []string{"os", "go"}) {
		t.Errorf("matrix keys = %v, want file order [os go]", test.Matrix.Keys)
	}
	if test.RunsOn.Image != DefaultImage || test.Timeout != DefaultTimeout {
		t.Errorf("defaults not applied: image=%q timeout=%v", test.RunsOn.Image, test.Timeout)
	}
	if r := test.Steps[1].Report; r == nil || r.Format != "junit" || !slices.Equal(r.Paths, []string{"build/*.xml"}) {
		t.Errorf("report shorthand = %+v", r)
	}

	rel := p.Jobs[1]
	if !slices.Equal(rel.Needs, []string{"test"}) {
		t.Errorf("needs shorthand = %v", rel.Needs)
	}
	if r := rel.RunsOn; r.OS != "macos" || r.Arch != "arm64" || r.Image != "xcode-16" {
		t.Errorf("runs-on = %+v", rel.RunsOn)
	}
	if rel.Timeout != 30*time.Minute {
		t.Errorf("timeout = %v", rel.Timeout)
	}
	if u := rel.Steps[1].Upload; u == nil || u.Name != "dist" {
		t.Errorf("upload = %+v", u)
	}
	if s := rel.Steps[2].Sign; s == nil || !slices.Equal(s.Paths, []string{"dist/*.dmg"}) {
		t.Errorf("sign shorthand = %+v", s)
	}
}

func TestShorthands(t *testing.T) {
	p := mustParse(t, `
on: [push, tag]
jobs:
  build:
    runs-on: linux
    steps:
      - run: make
`)
	if p.Triggers.Push == nil || p.Triggers.Tag == nil || p.Triggers.PullRequest != nil {
		t.Errorf("list triggers = %+v", p.Triggers)
	}
	if p.Jobs[0].RunsOn.OS != "linux" {
		t.Errorf("runs-on shorthand = %+v", p.Jobs[0].RunsOn)
	}

	p = mustParse(t, "on: push\njobs:\n  a:\n    runs-on: windows\n    steps: [{ run: dir }]\n")
	if p.Triggers.Push == nil {
		t.Error("scalar trigger not parsed")
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"empty", "", []string{"pipeline file is empty"}},
		{"syntax", "on: [push\njobs: {}", []string{"1:"}},
		{"no trigger", "jobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n", []string{"on: at least one trigger"}},
		{"no jobs", "on: push\n", []string{"jobs: at least one job"}},
		{"unknown top-level field", "on: push\nstages: []\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n",
			[]string{`2:1: unknown field "stages"`}},
		{"unknown job field", "on: push\njobs:\n  a:\n    runs-on: linux\n    stage: build\n    steps: [{run: x}]\n",
			[]string{`5:5: unknown field "stage"`}},
		{"unknown trigger", "on:\n  merge_request: {}\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n",
			[]string{`2:3: unknown trigger "merge_request"`}},
		{"unknown runs-on field", "on: push\njobs:\n  a:\n    runs-on: { os: linux, cpu: 4 }\n    steps: [{run: x}]\n",
			[]string{`unknown field "cpu"`}},
		{"bad os", "on: push\njobs:\n  a:\n    runs-on: solaris\n    steps: [{run: x}]\n",
			[]string{`4:14: jobs.a.runs-on.os: unknown os "solaris"`}},
		{"bad arch", "on: push\njobs:\n  a:\n    runs-on: { os: linux, arch: riscv }\n    steps: [{run: x}]\n",
			[]string{`unknown arch "riscv"`}},
		{"missing runs-on", "on: push\njobs:\n  a:\n    steps: [{run: x}]\n", []string{"jobs.a: runs-on is required"}},
		{"no steps", "on: push\njobs:\n  a:\n    runs-on: linux\n", []string{"at least one step"}},
		{"two actions", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - run: x\n        sign: y\n",
			[]string{"jobs.a.steps[0]: a step needs exactly one of run, report, upload or sign (found 2)"}},
		{"no action", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - name: nothing\n",
			[]string{"(found 0)"}},
		{"unknown need", "on: push\njobs:\n  a:\n    runs-on: linux\n    needs: [b]\n    steps: [{run: x}]\n",
			[]string{`unknown job "b"`}},
		{"cycle", "on: push\njobs:\n  a:\n    runs-on: linux\n    needs: c\n    steps: [{run: x}]\n" +
			"  b:\n    runs-on: linux\n    needs: a\n    steps: [{run: x}]\n" +
			"  c:\n    runs-on: linux\n    needs: b\n    steps: [{run: x}]\n",
			[]string{"dependency cycle: a -> c -> b -> a"}},
		{"bad condition", "on: push\njobs:\n  a:\n    runs-on: linux\n    if: event.branch ==\n    steps: [{run: x}]\n",
			[]string{"jobs.a.if: invalid expression"}},
		{"non-bool condition", "on: push\njobs:\n  a:\n    runs-on: linux\n    if: '\"main\"'\n    steps: [{run: x}]\n",
			[]string{"must evaluate to a bool"}},
		{"unknown matrix key", "on: push\njobs:\n  a:\n    matrix: { os: [linux] }\n    runs-on: ${{ matrix.oss }}\n    steps: [{run: x}]\n",
			[]string{`unknown matrix key "oss"`}},
		{"no matrix", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - run: echo ${{ matrix.os }}\n",
			[]string{"this job has no matrix"}},
		{"unknown event field", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - run: echo ${{ event.author }}\n",
			[]string{`unknown event field "author"`}},
		{"colon in a command", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - run: git log --format='%h: %s'\n",
			[]string{`hint: a value containing ": " must be quoted`}},
		{"expression in flow mapping", "on: push\njobs:\n  a:\n    matrix: { os: [linux] }\n    runs-on: { os: ${{ matrix.os }} }\n    steps: [{run: x}]\n",
			[]string{"hint: ${{ }} can't be used inside inline"}},
		{"quoted expression in flow mapping is fine", "on: push\njobs:\n  a:\n    matrix: { os: [linux] }\n    runs-on: { os: \"${{ matrix.os }}\", arch: sparc }\n    steps: [{run: x}]\n",
			[]string{`unknown arch "sparc"`}},
		{"secret not on job", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps:\n      - run: deploy\n        secrets: [KEY]\n",
			[]string{`secret "KEY" is not listed in the job's secrets`}},
		{"bad secret name", "on: push\njobs:\n  a:\n    runs-on: linux\n    secrets: [deploy-key]\n    steps: [{run: x}]\n",
			[]string{`invalid secret name "deploy-key"`}},
		{"bad timeout", "on: push\njobs:\n  a:\n    runs-on: linux\n    timeout: soon\n    steps: [{run: x}]\n",
			[]string{`invalid duration "soon"`}},
		{"bad report format", "on: push\njobs:\n  a:\n    runs-on: linux\n    steps: [{report: tap out.tap}]\n",
			[]string{`unknown report format "tap"`}},
		{"bad glob", "on:\n  push: { branches: ['[main'] }\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n",
			[]string{`invalid pattern "[main"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := parseErrors(t, tt.src)
			for _, w := range tt.want {
				wantError(t, errs, w)
			}
		})
	}
}

func TestReportsAllErrors(t *testing.T) {
	errs := parseErrors(t, `
on: push
jobs:
  a:
    runs-on: solaris
    steps: [{ run: x }]
  b:
    runs-on: linux
    timeout: never
    steps: []
`)
	if len(errs) != 3 {
		t.Errorf("got %d errors, want 3:\n%s", len(errs), strings.Join(errs, "\n"))
	}
}

// An unknown field must not hide the validation errors that follow it.
func TestUnknownFieldDoesNotHideOtherErrors(t *testing.T) {
	errs := parseErrors(t, `
on: push
jobs:
  build:
    runs-on: solaris
    steps:
      - run: make
        sign: dist/*
  deploy:
    needs: [biuld]
    runs-on: linux
    secret: [KEY]
    steps:
      - run: ./deploy.sh
`)
	want := []string{
		`5:14: jobs.build.runs-on.os: unknown os "solaris"`,
		`7:9: jobs.build.steps[0]: a step needs exactly one`,
		`10:12: jobs.deploy.needs: unknown job "biuld"`,
		`12:5: unknown field "secret"`,
	}
	if len(errs) != len(want) {
		t.Fatalf("got %d errors, want %d:\n%s", len(errs), len(want), strings.Join(errs, "\n"))
	}
	for i, w := range want {
		if !strings.HasPrefix(errs[i], w) {
			t.Errorf("error %d = %q, want prefix %q (errors are sorted by position)", i, errs[i], w)
		}
	}
}

const multiOS = `
on:
  push: { branches: [main, "feature/**"] }
  pull_request: { branches: [main] }
  tag: { pattern: "v*" }
env:
  CI: "true"
jobs:
  test:
    matrix: { os: [linux, windows, macos] }
    runs-on:
      os: ${{ matrix.os }}
    env:
      TARGET: ${{ matrix.os }}
    steps:
      - name: test on ${{ matrix.os }}
        run: make test SHA=${{ event.sha }}
  lint:
    runs-on: linux
    if: '!event.pull_request'
    steps: [{ run: make lint }]
  release:
    needs: [test, lint]
    if: event.tag != ""
    runs-on: macos
    steps: [{ run: make release }]
`

func TestPlanPush(t *testing.T) {
	p := mustParse(t, multiOS)
	plan, err := p.Plan(Event{Type: EventPush, Branch: "main", SHA: "abc123"})
	if err != nil {
		t.Fatal(err)
	}
	ids := plannedIDs(plan)
	want := []string{"test[os=linux]", "test[os=windows]", "test[os=macos]", "lint"}
	if !slices.Equal(ids, want) {
		t.Errorf("planned = %v, want %v", ids, want)
	}
	if !slices.Equal(plan.Skipped, []string{"release"}) {
		t.Errorf("skipped = %v, want [release]", plan.Skipped)
	}

	win := plan.Jobs[1]
	if win.RunsOn.OS != "windows" || win.Name != "test (windows)" {
		t.Errorf("windows job = %+v", win)
	}
	if win.Env["TARGET"] != "windows" || win.Env["CI"] != "true" {
		t.Errorf("env = %v", win.Env)
	}
	if got := win.Steps[0].Run.Command; got != "make test SHA=abc123" {
		t.Errorf("command = %q", got)
	}
	if win.Steps[0].Name != "test on windows" {
		t.Errorf("step name = %q", win.Steps[0].Name)
	}
}

func TestPlanTag(t *testing.T) {
	p := mustParse(t, multiOS)
	plan, err := p.Plan(Event{Type: EventTag, Tag: "v1.0.0", SHA: "def"})
	if err != nil {
		t.Fatal(err)
	}
	rel := plan.Jobs[len(plan.Jobs)-1]
	if rel.ID != "release" {
		t.Fatalf("last job = %s, want release", rel.ID)
	}
	want := []string{"test[os=linux]", "test[os=windows]", "test[os=macos]", "lint"}
	if !slices.Equal(rel.Needs, want) {
		t.Errorf("release needs = %v, want every test instance and lint", rel.Needs)
	}
}

func TestPlanSkipsDependents(t *testing.T) {
	p := mustParse(t, multiOS)
	// lint is skipped for pull requests, so release (which needs it) is too.
	plan, err := p.Plan(Event{Type: EventPullRequest, Branch: "fix", BaseBranch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(plan.Skipped, []string{"lint", "release"}) {
		t.Errorf("skipped = %v", plan.Skipped)
	}
}

func TestTriggerFilters(t *testing.T) {
	p := mustParse(t, multiOS)
	tests := []struct {
		ev   Event
		want bool
	}{
		{Event{Type: EventPush, Branch: "main"}, true},
		{Event{Type: EventPush, Branch: "feature/a/b"}, true},
		{Event{Type: EventPush, Branch: "develop"}, false},
		{Event{Type: EventPullRequest, Branch: "x", BaseBranch: "main"}, true},
		{Event{Type: EventPullRequest, Branch: "main", BaseBranch: "develop"}, false},
		{Event{Type: EventTag, Tag: "v2"}, true},
		{Event{Type: EventTag, Tag: "nightly"}, false},
	}
	for _, tt := range tests {
		if got := p.Matches(tt.ev); got != tt.want {
			t.Errorf("Matches(%+v) = %v, want %v", tt.ev, got, tt.want)
		}
	}
	plan, err := p.Plan(Event{Type: EventPush, Branch: "develop"})
	if err != nil || len(plan.Jobs) != 0 {
		t.Errorf("untriggered plan = %+v, %v; want empty", plan, err)
	}
}

func TestPlanInvalidInterpolatedOS(t *testing.T) {
	p := mustParse(t, `
on: push
jobs:
  a:
    matrix: { os: [linux, beos] }
    runs-on: ${{ matrix.os }}
    steps: [{ run: x }]
`)
	_, err := p.Plan(Event{Type: EventPush, Branch: "main"})
	if err == nil || !strings.Contains(err.Error(), `a[os=beos]: unknown os "beos"`) {
		t.Errorf("err = %v", err)
	}
}

func TestMatrixCombinations(t *testing.T) {
	p := mustParse(t, `
on: push
jobs:
  a:
    matrix: { os: [linux, macos], go: ["1.26", "1.27"] }
    runs-on: ${{ matrix.os }}
    steps: [{ run: go test }]
`)
	plan, err := p.Plan(Event{Type: EventPush})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a[os=linux,go=1.26]", "a[os=linux,go=1.27]", "a[os=macos,go=1.26]", "a[os=macos,go=1.27]"}
	if got := plannedIDs(plan); !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func jobIDs(p *Pipeline) []string {
	var ids []string
	for _, j := range p.Jobs {
		ids = append(ids, j.ID)
	}
	return ids
}

func plannedIDs(p *Plan) []string {
	var ids []string
	for _, j := range p.Jobs {
		ids = append(ids, j.ID)
	}
	return ids
}
