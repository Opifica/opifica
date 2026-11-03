package pipeline

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// The raw* types mirror the YAML file. They accept the shorthand forms of the
// language and are turned into the public types by Parse.

type rawPipeline struct {
	On   *rawTriggers       `yaml:"on"`
	Env  map[string]string  `yaml:"env"`
	Jobs map[string]*rawJob `yaml:"jobs"`
}

type rawJob struct {
	Name    string                `yaml:"name"`
	RunsOn  *rawRunsOn            `yaml:"runs-on"`
	Matrix  map[string]stringList `yaml:"matrix"`
	Needs   stringList            `yaml:"needs"`
	If      string                `yaml:"if"`
	Env     map[string]string     `yaml:"env"`
	Secrets stringList            `yaml:"secrets"`
	Timeout string                `yaml:"timeout"`
	Steps   []rawStep             `yaml:"steps"`
}

type rawStep struct {
	Name             string            `yaml:"name"`
	Run              *string           `yaml:"run"`
	Shell            string            `yaml:"shell"`
	WorkingDirectory string            `yaml:"working-directory"`
	Report           *rawReport        `yaml:"report"`
	Upload           *rawUpload        `yaml:"upload"`
	Sign             *rawSign          `yaml:"sign"`
	Env              map[string]string `yaml:"env"`
	Secrets          stringList        `yaml:"secrets"`
	ContinueOnError  bool              `yaml:"continue-on-error"`
	Timeout          string            `yaml:"timeout"`
}

// typeErr reports a problem in a custom unmarshaler. yaml.v3 collects
// TypeErrors and keeps decoding, so the user sees every problem at once.
func typeErr(n *yaml.Node, format string, args ...any) error {
	return &yaml.TypeError{Errors: []string{
		fmt.Sprintf("line %d:%d: %s", n.Line, n.Column, fmt.Sprintf(format, args...)),
	}}
}

// checkKeys reports keys of a mapping node that aren't in allowed.
func checkKeys(n *yaml.Node, allowed ...string) error {
	var errs []string
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i]
		ok := false
		for _, a := range allowed {
			if k.Value == a {
				ok = true
				break
			}
		}
		if !ok {
			errs = append(errs, fmt.Sprintf("line %d:%d: unknown field %q (expected one of: %s)",
				k.Line, k.Column, k.Value, strings.Join(allowed, ", ")))
		}
	}
	if len(errs) > 0 {
		return &yaml.TypeError{Errors: errs}
	}
	return nil
}

func isNull(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Tag == "!!null"
}

// stringList accepts a single string or a list of strings.
type stringList []string

func (l *stringList) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		if isNull(n) {
			*l = nil
			return nil
		}
		*l = stringList{n.Value}
		return nil
	case yaml.SequenceNode:
		out := make(stringList, 0, len(n.Content))
		for _, item := range n.Content {
			if item.Kind != yaml.ScalarNode {
				return typeErr(item, "expected a string")
			}
			out = append(out, item.Value)
		}
		*l = out
		return nil
	}
	return typeErr(n, "expected a string or a list of strings")
}

// rawTriggers accepts:
//
//	on: push
//	on: [push, pull_request]
//	on:
//	  push: { branches: [main] }
//	  pull_request:
//	  tag: { pattern: "v*" }
type rawTriggers struct {
	Push        *BranchFilter
	PullRequest *BranchFilter
	Tag         *TagFilter
}

var triggerNames = []string{"push", "pull_request", "tag"}

func (t *rawTriggers) set(n *yaml.Node, name string, value *yaml.Node) error {
	switch name {
	case "push", "pull_request":
		f := &BranchFilter{}
		if value != nil && !isNull(value) {
			if value.Kind != yaml.MappingNode {
				return typeErr(value, "%s: expected a mapping, e.g. { branches: [main] }", name)
			}
			if err := checkKeys(value, "branches"); err != nil {
				return err
			}
			var v struct {
				Branches stringList `yaml:"branches"`
			}
			if err := value.Decode(&v); err != nil {
				return err
			}
			f.Branches = v.Branches
		}
		if name == "push" {
			t.Push = f
		} else {
			t.PullRequest = f
		}
	case "tag":
		f := &TagFilter{}
		if value != nil && !isNull(value) {
			if value.Kind != yaml.MappingNode {
				return typeErr(value, `tag: expected a mapping, e.g. { pattern: "v*" }`)
			}
			if err := checkKeys(value, "pattern"); err != nil {
				return err
			}
			var v struct {
				Pattern stringList `yaml:"pattern"`
			}
			if err := value.Decode(&v); err != nil {
				return err
			}
			f.Patterns = v.Pattern
		}
		t.Tag = f
	default:
		return typeErr(n, "unknown trigger %q (expected one of: %s)", name, strings.Join(triggerNames, ", "))
	}
	return nil
}

func (t *rawTriggers) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		if isNull(n) {
			return nil
		}
		return t.set(n, n.Value, nil)
	case yaml.SequenceNode:
		for _, item := range n.Content {
			if item.Kind != yaml.ScalarNode {
				return typeErr(item, "expected a trigger name")
			}
			if err := t.set(item, item.Value, nil); err != nil {
				return err
			}
		}
		return nil
	case yaml.MappingNode:
		var errs []string
		for i := 0; i+1 < len(n.Content); i += 2 {
			if err := t.set(n.Content[i], n.Content[i].Value, n.Content[i+1]); err != nil {
				te, ok := err.(*yaml.TypeError)
				if !ok {
					return err
				}
				errs = append(errs, te.Errors...)
			}
		}
		if len(errs) > 0 {
			return &yaml.TypeError{Errors: errs}
		}
		return nil
	}
	return typeErr(n, "expected a trigger name, a list of triggers or a mapping")
}

// rawRunsOn accepts `runs-on: linux` or a mapping with os, arch, image, labels.
type rawRunsOn struct {
	OS     string            `yaml:"os"`
	Arch   string            `yaml:"arch"`
	Image  string            `yaml:"image"`
	Labels map[string]string `yaml:"labels"`
}

func (r *rawRunsOn) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		r.OS = n.Value
		return nil
	case yaml.MappingNode:
		if err := checkKeys(n, "os", "arch", "image", "labels"); err != nil {
			return err
		}
		type plain rawRunsOn
		return n.Decode((*plain)(r))
	}
	return typeErr(n, "expected an OS name or a mapping with os, arch, image and labels")
}

// rawReport accepts `report: junit build/*.xml` or { format, paths }.
type rawReport struct {
	Format string     `yaml:"format"`
	Paths  stringList `yaml:"paths"`
}

func (r *rawReport) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		fields := strings.Fields(n.Value)
		if len(fields) < 2 {
			return typeErr(n, `report: expected "<format> <paths...>", e.g. "junit build/*.xml"`)
		}
		r.Format, r.Paths = fields[0], fields[1:]
		return nil
	case yaml.MappingNode:
		if err := checkKeys(n, "format", "paths"); err != nil {
			return err
		}
		type plain rawReport
		return n.Decode((*plain)(r))
	}
	return typeErr(n, "report: expected a string or a mapping with format and paths")
}

// rawUpload accepts `upload: dist/*` or { name, paths }.
type rawUpload struct {
	Name  string     `yaml:"name"`
	Paths stringList `yaml:"paths"`
}

func (u *rawUpload) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode, yaml.SequenceNode:
		return n.Decode(&u.Paths)
	case yaml.MappingNode:
		if err := checkKeys(n, "name", "paths"); err != nil {
			return err
		}
		type plain rawUpload
		return n.Decode((*plain)(u))
	}
	return typeErr(n, "upload: expected paths or a mapping with name and paths")
}

// rawSign accepts `sign: dist/*.dmg`, a list of paths, or { paths, profile }.
type rawSign struct {
	Paths   stringList `yaml:"paths"`
	Profile string     `yaml:"profile"`
}

func (s *rawSign) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode, yaml.SequenceNode:
		return n.Decode(&s.Paths)
	case yaml.MappingNode:
		if err := checkKeys(n, "paths", "profile"); err != nil {
			return err
		}
		type plain rawSign
		return n.Decode((*plain)(s))
	}
	return typeErr(n, "sign: expected paths or a mapping with paths and profile")
}
