package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

var (
	idRe        = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*$`)
	matrixKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	envKeyRe    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	secretRe    = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	yamlErrRe   = regexp.MustCompile(`^(?:yaml: )?line (\d+)(?::(\d+))?: (.*)$`)
	unknownRe   = regexp.MustCompile(`^field (\S+) not found in type \S+$`)
	// unknownFieldRe matches messages produced from unknownRe.
	unknownFieldRe = regexp.MustCompile(`^unknown field "(.*)"$`)
)

// Supported values for runs-on.
var (
	OperatingSystems = []string{"linux", "windows", "macos"}
	Architectures    = []string{"amd64", "arm64"}
	ReportFormats    = []string{"junit"}
)

// Parse parses and validates a pipeline file. On failure it returns Errors,
// listing every problem with its line and column.
func Parse(data []byte) (*Pipeline, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, Errors{{Msg: "pipeline file is empty"}}
	}

	// The node tree gives positions for validation errors and the order of
	// keys, which Go maps don't keep.
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		errs := convertYAMLError(err)
		addFlowHint(errs, data)
		addColonHint(errs)
		return nil, errs
	}

	var raw rawPipeline
	var decodeErrs Errors
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		decodeErrs = convertYAMLError(err)
		addColumns(decodeErrs, &root)
		// A TypeError still leaves a partly decoded pipeline, so validate
		// it too and report every problem at once. Anything else is fatal.
		var te *yaml.TypeError
		if !errors.As(err, &te) {
			return nil, decodeErrs
		}
	}

	c := &checker{root: docNode(&root), errs: decodeErrs}
	p := c.build(&raw)
	if len(c.errs) > 0 {
		slices.SortStableFunc(c.errs, func(a, b *Error) int {
			if a.Line != b.Line {
				return a.Line - b.Line
			}
			return a.Column - b.Column
		})
		return nil, c.errs
	}
	return p, nil
}

func convertYAMLError(err error) Errors {
	var lines []string
	var te *yaml.TypeError
	if errors.As(err, &te) {
		lines = te.Errors
	} else {
		lines = []string{err.Error()}
	}
	errs := make(Errors, 0, len(lines))
	for _, l := range lines {
		e := &Error{Msg: l}
		if m := yamlErrRe.FindStringSubmatch(l); m != nil {
			e.Line, _ = strconv.Atoi(m[1])
			e.Column, _ = strconv.Atoi(m[2])
			e.Msg = m[3]
		}
		if m := unknownRe.FindStringSubmatch(e.Msg); m != nil {
			e.Msg = fmt.Sprintf("unknown field %q", m[1])
		}
		errs = append(errs, e)
	}
	return errs
}

// addFlowHint explains the most common syntax mistake: ${{ }} inside an
// inline { } or [ ] collection, where YAML reads the braces as structure.
func addFlowHint(errs Errors, data []byte) {
	lines := strings.Split(string(data), "\n")
	for _, e := range errs {
		if e.Line < 1 || e.Line > len(lines) {
			continue
		}
		// YAML may report the error a line before or after the offending one.
		for _, l := range lines[max(0, e.Line-2):min(len(lines), e.Line+1)] {
			expr, open := strings.Index(l, "${{"), strings.IndexAny(l, "{[")
			if expr >= 0 && open >= 0 && open < expr {
				e.Msg += ` (hint: ${{ }} can't be used inside inline { } or [ ]; put the value on its own line or quote it, e.g. os: "${{ matrix.os }}")`
				break
			}
		}
	}
}

// addColonHint explains "mapping values are not allowed": usually a command
// or value containing ": ", which YAML reads as a nested key.
func addColonHint(errs Errors) {
	for _, e := range errs {
		if strings.Contains(e.Msg, "mapping values are not allowed") && !strings.Contains(e.Msg, "hint:") {
			e.Msg += ` (hint: a value containing ": " must be quoted, or written as a block, e.g. run: | on its own line with the command indented below)`
		}
	}
}

// addColumns fills in columns yaml.v3 leaves out of decode errors, using the
// key on the reported line.
func addColumns(errs Errors, root *yaml.Node) {
	for _, e := range errs {
		if e.Line == 0 || e.Column > 0 {
			continue
		}
		want := ""
		if m := unknownFieldRe.FindStringSubmatch(e.Msg); m != nil {
			want = m[1]
		}
		var walk func(n *yaml.Node)
		walk = func(n *yaml.Node) {
			if n.Line == e.Line && (want == "" || n.Value == want) && (e.Column == 0 || n.Column < e.Column) {
				e.Column = n.Column
			}
			for _, c := range n.Content {
				walk(c)
			}
		}
		walk(root)
	}
}

func docNode(n *yaml.Node) *yaml.Node {
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	return n
}

// checker builds the Pipeline and collects errors with positions.
type checker struct {
	root *yaml.Node
	errs Errors
}

// path elements are strings (mapping keys) or ints (sequence indexes).
type path []any

func (p path) String() string {
	var b strings.Builder
	for _, e := range p {
		switch e := e.(type) {
		case string:
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			b.WriteString(e)
		case int:
			fmt.Fprintf(&b, "[%d]", e)
		}
	}
	return b.String()
}

func (p path) with(e ...any) path {
	return append(slices.Clip(p), e...)
}

// locate returns the node at p, or the deepest existing node on the way.
func (c *checker) locate(p path) *yaml.Node {
	n, _ := c.find(p)
	return n
}

// find walks p and reports whether every element was found.
func (c *checker) find(p path) (*yaml.Node, bool) {
	n := c.root
	for _, e := range p {
		var next *yaml.Node
		switch e := e.(type) {
		case string:
			if n.Kind == yaml.MappingNode {
				for i := 0; i+1 < len(n.Content); i += 2 {
					if n.Content[i].Value == e {
						next = n.Content[i+1]
						break
					}
				}
			}
		case int:
			if n.Kind == yaml.SequenceNode && e < len(n.Content) {
				next = n.Content[e]
			}
		}
		if next == nil {
			return n, false
		}
		n = next
	}
	return n, true
}

// keys returns the keys of the mapping at p in file order.
func (c *checker) keys(p path) []string {
	n, ok := c.find(p)
	if !ok || n.Kind != yaml.MappingNode {
		return nil
	}
	ks := make([]string, 0, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		ks = append(ks, n.Content[i].Value)
	}
	return ks
}

func (c *checker) add(p path, format string, args ...any) {
	n := c.locate(p)
	c.errs = append(c.errs, &Error{
		Line: n.Line, Column: n.Column, Path: p.String(), Msg: fmt.Sprintf(format, args...),
	})
}

func (c *checker) build(raw *rawPipeline) *Pipeline {
	p := &Pipeline{Env: raw.Env}

	if raw.On == nil || (raw.On.Push == nil && raw.On.PullRequest == nil && raw.On.Tag == nil) {
		c.add(path{"on"}, "at least one trigger is required (push, pull_request or tag)")
	} else {
		p.Triggers = Triggers{Push: raw.On.Push, PullRequest: raw.On.PullRequest, Tag: raw.On.Tag}
		c.checkGlobs(path{"on", "push", "branches"}, p.Triggers.Push)
		c.checkGlobs(path{"on", "pull_request", "branches"}, p.Triggers.PullRequest)
		if t := p.Triggers.Tag; t != nil {
			for _, pat := range t.Patterns {
				c.checkGlob(path{"on", "tag", "pattern"}, pat)
			}
		}
	}
	c.checkEnv(path{"env"}, p.Env, nil)

	if len(raw.Jobs) == 0 {
		c.add(path{"jobs"}, "at least one job is required")
		return p
	}
	for _, id := range c.keys(path{"jobs"}) {
		rj := raw.Jobs[id]
		if rj == nil {
			c.add(path{"jobs", id}, "job is empty")
			continue
		}
		p.Jobs = append(p.Jobs, c.buildJob(path{"jobs", id}, id, rj))
	}
	c.checkNeeds(p)
	return p
}

func (c *checker) checkGlobs(p path, f *BranchFilter) {
	if f == nil {
		return
	}
	for _, g := range f.Branches {
		c.checkGlob(p, g)
	}
}

func (c *checker) checkGlob(p path, pattern string) {
	if _, err := matchGlob(pattern, ""); err != nil {
		c.add(p, "invalid pattern %q: %v", pattern, err)
	}
}

func (c *checker) checkEnv(p path, env map[string]string, matrixKeys []string) {
	for k, v := range env {
		if !envKeyRe.MatchString(k) {
			c.add(p.with(k), "invalid variable name %q", k)
		}
		c.checkText(p.with(k), v, matrixKeys)
	}
}

func (c *checker) checkText(p path, s string, matrixKeys []string) {
	for _, msg := range checkInterpolations(s, matrixKeys) {
		c.add(p, "%s", msg)
	}
}

func (c *checker) checkSecretNames(p path, names []string) {
	for _, s := range names {
		if !secretRe.MatchString(s) {
			c.add(p, "invalid secret name %q (use UPPER_SNAKE_CASE)", s)
		}
	}
}

func (c *checker) duration(p path, s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		c.add(p, "invalid duration %q (use e.g. 30m or 1h30m)", s)
		return 0
	}
	return d
}

func (c *checker) buildJob(p path, id string, rj *rawJob) *Job {
	if !idRe.MatchString(id) {
		c.add(p, "invalid job id %q (letters, digits, - and _, starting with a letter)", id)
	}
	j := &Job{
		ID:      id,
		Name:    rj.Name,
		Needs:   rj.Needs,
		If:      strings.TrimSpace(rj.If),
		Env:     rj.Env,
		Secrets: rj.Secrets,
		Timeout: c.duration(p.with("timeout"), rj.Timeout),
	}
	if j.Timeout == 0 {
		j.Timeout = DefaultTimeout
	}

	// Matrix first: other fields may refer to its keys.
	if len(rj.Matrix) > 0 {
		j.Matrix.Values = map[string][]string{}
		for _, k := range c.keys(p.with("matrix")) {
			vals := rj.Matrix[k]
			if !matrixKeyRe.MatchString(k) {
				c.add(p.with("matrix", k), "invalid matrix key %q (lowercase letters, digits and _)", k)
			}
			if len(vals) == 0 {
				c.add(p.with("matrix", k), "matrix key %q needs at least one value", k)
			}
			if dup := firstDuplicate(vals); dup != "" {
				c.add(p.with("matrix", k), "duplicate matrix value %q", dup)
			}
			j.Matrix.Keys = append(j.Matrix.Keys, k)
			j.Matrix.Values[k] = vals
		}
	}
	mk := j.Matrix.Keys

	c.checkText(p.with("name"), j.Name, mk)
	c.checkEnv(p.with("env"), j.Env, mk)
	c.checkSecretNames(p.with("secrets"), j.Secrets)

	if rj.RunsOn == nil {
		c.add(p, "runs-on is required, e.g. runs-on: linux")
	} else {
		j.RunsOn = RunsOn{OS: rj.RunsOn.OS, Arch: rj.RunsOn.Arch, Image: rj.RunsOn.Image, Labels: rj.RunsOn.Labels}
		if j.RunsOn.Image == "" {
			j.RunsOn.Image = DefaultImage
		}
		c.checkRunsOn(p.with("runs-on"), j.RunsOn, mk)
	}

	if j.If != "" {
		prog, err := compileCondition(j.If)
		if err != nil {
			c.add(p.with("if"), "%v", err)
		}
		j.ifProgram = prog
	}

	if len(rj.Steps) == 0 {
		c.add(p, "at least one step is required")
	}
	for i := range rj.Steps {
		j.Steps = append(j.Steps, c.buildStep(p.with("steps", i), &rj.Steps[i], j, mk))
	}
	return j
}

// checkRunsOn validates runs-on. Values containing ${{ }} are checked again
// after substitution, when the run is planned.
func (c *checker) checkRunsOn(p path, r RunsOn, mk []string) {
	switch {
	case strings.Contains(r.OS, "${{"):
		c.checkText(p.with("os"), r.OS, mk)
	case r.OS == "":
		c.add(p, "os is required (one of: %s)", strings.Join(OperatingSystems, ", "))
	case !slices.Contains(OperatingSystems, r.OS):
		c.add(p.with("os"), "unknown os %q (one of: %s)", r.OS, strings.Join(OperatingSystems, ", "))
	}
	switch {
	case strings.Contains(r.Arch, "${{"):
		c.checkText(p.with("arch"), r.Arch, mk)
	case r.Arch != "" && !slices.Contains(Architectures, r.Arch):
		c.add(p.with("arch"), "unknown arch %q (one of: %s)", r.Arch, strings.Join(Architectures, ", "))
	}
	c.checkText(p.with("image"), r.Image, mk)
	for k, v := range r.Labels {
		c.checkText(p.with("labels", k), v, mk)
	}
}

func (c *checker) buildStep(p path, rs *rawStep, j *Job, mk []string) Step {
	s := Step{
		Name:            rs.Name,
		Env:             rs.Env,
		Secrets:         rs.Secrets,
		ContinueOnError: rs.ContinueOnError,
		Timeout:         c.duration(p.with("timeout"), rs.Timeout),
	}
	actions := 0
	if rs.Run != nil {
		actions++
		s.Run = &RunAction{Command: *rs.Run, Shell: rs.Shell, WorkingDirectory: rs.WorkingDirectory}
		if strings.TrimSpace(*rs.Run) == "" {
			c.add(p.with("run"), "command is empty")
		}
		c.checkText(p.with("run"), *rs.Run, mk)
		c.checkText(p.with("working-directory"), rs.WorkingDirectory, mk)
	} else if rs.Shell != "" || rs.WorkingDirectory != "" {
		c.add(p, "shell and working-directory only apply to run steps")
	}
	if rs.Report != nil {
		actions++
		s.Report = &ReportAction{Format: rs.Report.Format, Paths: rs.Report.Paths}
		if !slices.Contains(ReportFormats, s.Report.Format) {
			c.add(p.with("report"), "unknown report format %q (one of: %s)", s.Report.Format, strings.Join(ReportFormats, ", "))
		}
		c.checkPaths(p.with("report"), s.Report.Paths, mk)
	}
	if rs.Upload != nil {
		actions++
		s.Upload = &UploadAction{Name: rs.Upload.Name, Paths: rs.Upload.Paths}
		if s.Upload.Name == "" {
			s.Upload.Name = "artifacts"
		}
		c.checkPaths(p.with("upload"), s.Upload.Paths, mk)
	}
	if rs.Sign != nil {
		actions++
		s.Sign = &SignAction{Paths: rs.Sign.Paths, Profile: rs.Sign.Profile}
		c.checkPaths(p.with("sign"), s.Sign.Paths, mk)
	}
	if actions != 1 {
		c.add(p, "a step needs exactly one of run, report, upload or sign (found %d)", actions)
	}

	c.checkText(p.with("name"), s.Name, mk)
	c.checkEnv(p.with("env"), s.Env, mk)
	c.checkSecretNames(p.with("secrets"), s.Secrets)
	for _, name := range s.Secrets {
		if !slices.Contains(j.Secrets, name) {
			c.add(p.with("secrets"), "secret %q is not listed in the job's secrets", name)
		}
	}
	return s
}

func (c *checker) checkPaths(p path, paths []string, mk []string) {
	if len(paths) == 0 {
		c.add(p, "at least one path is required")
	}
	for _, s := range paths {
		c.checkText(p, s, mk)
	}
}

// checkNeeds validates job dependencies and rejects cycles.
func (c *checker) checkNeeds(p *Pipeline) {
	byID := map[string]*Job{}
	for _, j := range p.Jobs {
		byID[j.ID] = j
	}
	valid := true
	for _, j := range p.Jobs {
		for _, n := range j.Needs {
			switch {
			case n == j.ID:
				c.add(path{"jobs", j.ID, "needs"}, "job cannot depend on itself")
				valid = false
			case byID[n] == nil:
				c.add(path{"jobs", j.ID, "needs"}, "unknown job %q", n)
				valid = false
			}
		}
	}
	if !valid {
		return
	}
	if _, cycle := topoSort(p.Jobs); cycle != nil {
		c.add(path{"jobs", cycle[0], "needs"}, "dependency cycle: %s", strings.Join(cycle, " -> "))
	}
}

// topoSort orders jobs so each comes after the jobs it needs, keeping file
// order where possible. It returns a cycle if there is one.
func topoSort(jobs []*Job) ([]*Job, []string) {
	byID := map[string]*Job{}
	for _, j := range jobs {
		byID[j.ID] = j
	}
	const (
		unvisited = iota
		visiting
		done
	)
	state := map[string]int{}
	var order []*Job
	var stack []string
	var cycle []string
	var visit func(j *Job) bool
	visit = func(j *Job) bool {
		switch state[j.ID] {
		case done:
			return true
		case visiting:
			i := slices.Index(stack, j.ID)
			cycle = append(slices.Clone(stack[i:]), j.ID)
			return false
		}
		state[j.ID] = visiting
		stack = append(stack, j.ID)
		for _, n := range j.Needs {
			if dep := byID[n]; dep != nil && !visit(dep) {
				return false
			}
		}
		stack = stack[:len(stack)-1]
		state[j.ID] = done
		order = append(order, j)
		return true
	}
	for _, j := range jobs {
		if !visit(j) {
			return nil, cycle
		}
	}
	return order, nil
}

func firstDuplicate(vals []string) string {
	seen := map[string]bool{}
	for _, v := range vals {
		if seen[v] {
			return v
		}
		seen[v] = true
	}
	return ""
}
