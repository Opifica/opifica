package pipeline

import (
	"fmt"
	"maps"
	gopath "path"
	"slices"
	"strings"
	"time"
)

// EventType is the kind of forge event that may start a run.
type EventType string

const (
	EventPush        EventType = "push"
	EventPullRequest EventType = "pull_request"
	EventTag         EventType = "tag"
)

// Event is a concrete forge event, always pinned to one commit.
type Event struct {
	Type EventType
	// Branch is the pushed branch, or the source branch of a pull request.
	Branch string
	// BaseBranch is the target branch of a pull request.
	BaseBranch string
	Tag        string
	SHA        string
	Ref        string
}

func (e Event) vars() map[string]any {
	return map[string]any{
		"type":         string(e.Type),
		"branch":       e.Branch,
		"base_branch":  e.BaseBranch,
		"tag":          e.Tag,
		"sha":          e.SHA,
		"ref":          e.Ref,
		"pull_request": e.Type == EventPullRequest,
	}
}

// Plan is the result of planning a pipeline for one event.
type Plan struct {
	// Jobs to schedule, ordered so every job comes after the jobs it needs.
	Jobs []*PlannedJob
	// Skipped lists job instances whose condition was false, or which needed
	// a job that was skipped.
	Skipped []string
}

// PlannedJob is one job instance ready to schedule: matrix expanded,
// condition evaluated and ${{ }} substituted.
type PlannedJob struct {
	// ID is unique within the plan, e.g. "test" or "test[os=linux]".
	ID string
	// JobID is the job's key in the pipeline file.
	JobID string
	// Name is for display, e.g. "test (linux)".
	Name    string
	Matrix  map[string]string
	RunsOn  RunsOn
	Needs   []string // IDs of planned jobs
	Env     map[string]string
	Secrets []string
	Timeout time.Duration
	Steps   []Step
}

// Matches reports whether the event triggers this pipeline.
func (p *Pipeline) Matches(ev Event) bool {
	switch ev.Type {
	case EventPush:
		return p.Triggers.Push != nil && matchAny(p.Triggers.Push.Branches, ev.Branch)
	case EventPullRequest:
		return p.Triggers.PullRequest != nil && matchAny(p.Triggers.PullRequest.Branches, ev.BaseBranch)
	case EventTag:
		return p.Triggers.Tag != nil && matchAny(p.Triggers.Tag.Patterns, ev.Tag)
	}
	return false
}

// Plan returns the jobs to run for ev. If the event doesn't trigger the
// pipeline, the plan is empty.
func (p *Pipeline) Plan(ev Event) (*Plan, error) {
	plan := &Plan{}
	if !p.Matches(ev) {
		return plan, nil
	}
	order, cycle := topoSort(p.Jobs)
	if cycle != nil {
		// Parse rejects cycles; this only happens for hand-built pipelines.
		return nil, Errors{{Msg: "dependency cycle: " + strings.Join(cycle, " -> ")}}
	}

	eventVars := ev.vars()
	planned := map[string][]string{} // job ID -> planned instance IDs
	var errs Errors

	for _, j := range order {
		combos := expand(j.Matrix)

		var missing string
		for _, n := range j.Needs {
			if len(planned[n]) == 0 {
				missing = n
				break
			}
		}

		for _, combo := range combos {
			id, name := instanceID(j, combo)
			if missing != "" {
				plan.Skipped = append(plan.Skipped, id)
				continue
			}
			vars := map[string]any{"event": eventVars, "matrix": combo}

			if j.ifProgram != nil {
				ok, err := evalCondition(j.ifProgram, vars)
				if err != nil {
					errs = append(errs, &Error{Path: "jobs." + j.ID + ".if", Msg: fmt.Sprintf("%s: %v", id, err)})
					continue
				}
				if !ok {
					plan.Skipped = append(plan.Skipped, id)
					continue
				}
			}

			pj := &PlannedJob{
				ID:      id,
				JobID:   j.ID,
				Name:    interpolate(name, vars),
				Matrix:  combo,
				Secrets: slices.Clone(j.Secrets),
				Timeout: j.Timeout,
				RunsOn: RunsOn{
					OS:     interpolate(j.RunsOn.OS, vars),
					Arch:   interpolate(j.RunsOn.Arch, vars),
					Image:  interpolate(j.RunsOn.Image, vars),
					Labels: interpolateMap(j.RunsOn.Labels, vars),
				},
			}
			if !slices.Contains(OperatingSystems, pj.RunsOn.OS) {
				errs = append(errs, &Error{Path: "jobs." + j.ID + ".runs-on.os",
					Msg: fmt.Sprintf("%s: unknown os %q (one of: %s)", id, pj.RunsOn.OS, strings.Join(OperatingSystems, ", "))})
			}
			if pj.RunsOn.Arch != "" && !slices.Contains(Architectures, pj.RunsOn.Arch) {
				errs = append(errs, &Error{Path: "jobs." + j.ID + ".runs-on.arch",
					Msg: fmt.Sprintf("%s: unknown arch %q (one of: %s)", id, pj.RunsOn.Arch, strings.Join(Architectures, ", "))})
			}

			// Job env overrides pipeline env.
			pj.Env = interpolateMap(p.Env, vars)
			if pj.Env == nil && j.Env != nil {
				pj.Env = map[string]string{}
			}
			maps.Copy(pj.Env, interpolateMap(j.Env, vars))

			for _, n := range j.Needs {
				pj.Needs = append(pj.Needs, planned[n]...)
			}
			for _, s := range j.Steps {
				pj.Steps = append(pj.Steps, interpolateStep(s, vars))
			}

			plan.Jobs = append(plan.Jobs, pj)
			planned[j.ID] = append(planned[j.ID], id)
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return plan, nil
}

func interpolateStep(s Step, vars map[string]any) Step {
	out := s
	out.Name = interpolate(s.Name, vars)
	out.Env = interpolateMap(s.Env, vars)
	out.Secrets = slices.Clone(s.Secrets)
	if s.Run != nil {
		r := *s.Run
		r.Command = interpolate(r.Command, vars)
		r.WorkingDirectory = interpolate(r.WorkingDirectory, vars)
		out.Run = &r
	}
	if s.Report != nil {
		r := *s.Report
		r.Paths = interpolateList(r.Paths, vars)
		out.Report = &r
	}
	if s.Upload != nil {
		u := *s.Upload
		u.Name = interpolate(u.Name, vars)
		u.Paths = interpolateList(u.Paths, vars)
		out.Upload = &u
	}
	if s.Sign != nil {
		sg := *s.Sign
		sg.Paths = interpolateList(sg.Paths, vars)
		out.Sign = &sg
	}
	return out
}

// expand returns every combination of matrix values, in file order. A job
// without a matrix has exactly one, empty combination.
func expand(m Matrix) []map[string]string {
	combos := []map[string]string{{}}
	for _, k := range m.Keys {
		var next []map[string]string
		for _, c := range combos {
			for _, v := range m.Values[k] {
				n := maps.Clone(c)
				n[k] = v
				next = append(next, n)
			}
		}
		combos = next
	}
	return combos
}

func instanceID(j *Job, combo map[string]string) (id, name string) {
	name = j.Name
	if name == "" {
		name = j.ID
	}
	if len(combo) == 0 {
		return j.ID, name
	}
	kv := make([]string, 0, len(combo))
	vals := make([]string, 0, len(combo))
	for _, k := range j.Matrix.Keys {
		kv = append(kv, k+"="+combo[k])
		vals = append(vals, combo[k])
	}
	return j.ID + "[" + strings.Join(kv, ",") + "]", name + " (" + strings.Join(vals, ", ") + ")"
}

// matchGlob matches a branch or tag name. "*" matches within one path
// segment ("release/*" matches "release/1.0"); a trailing "/**" matches
// everything below a prefix ("feature/**" matches "feature/a/b").
func matchGlob(pattern, name string) (bool, error) {
	if prefix, ok := strings.CutSuffix(pattern, "/**"); ok {
		if _, err := gopath.Match(prefix, ""); err != nil {
			return false, err
		}
		return name == prefix || strings.HasPrefix(name, prefix+"/"), nil
	}
	return gopath.Match(pattern, name)
}

// matchAny reports whether name matches any pattern. No patterns match all.
func matchAny(patterns []string, name string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if ok, _ := matchGlob(p, name); ok {
			return true
		}
	}
	return false
}
