package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"cel.dev/cel-go/cel"
)

// Expressions come in two forms:
//
//   - `if:` holds a CEL expression that must evaluate to a bool, e.g.
//     `event.branch == "main" && matrix.os != "windows"`.
//   - Strings may contain ${{ matrix.<key> }} or ${{ event.<field> }}, which
//     are substituted when the run is planned.
//
// Both see the same two variables: `event` (fields below) and `matrix`.

// eventFields are the fields available as event.<field>.
var eventFields = []string{"type", "branch", "base_branch", "tag", "sha", "ref", "pull_request"}

var (
	celEnvOnce sync.Once
	celEnv     *cel.Env
	celEnvErr  error
)

func exprEnv() (*cel.Env, error) {
	celEnvOnce.Do(func() {
		celEnv, celEnvErr = cel.NewEnv(
			cel.Variable("event", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("matrix", cel.MapType(cel.StringType, cel.StringType)),
		)
	})
	return celEnv, celEnvErr
}

// compileCondition compiles an `if:` expression and checks that it yields a bool.
func compileCondition(src string) (cel.Program, error) {
	env, err := exprEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(src)
	if iss.Err() != nil {
		return nil, fmt.Errorf("invalid expression: %s", strings.TrimSpace(iss.Err().Error()))
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("expression must evaluate to a bool, not %s", t)
	}
	return env.Program(ast)
}

func evalCondition(p cel.Program, vars map[string]any) (bool, error) {
	out, _, err := p.Eval(vars)
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("expression evaluated to %v, not a bool", out.Value())
	}
	return b, nil
}

var interpRe = regexp.MustCompile(`\$\{\{\s*(.*?)\s*\}\}`)

// checkInterpolations validates every ${{ }} in s against the matrix keys.
func checkInterpolations(s string, matrixKeys []string) []string {
	var problems []string
	for _, m := range interpRe.FindAllStringSubmatch(s, -1) {
		if msg := checkReference(m[1], matrixKeys); msg != "" {
			problems = append(problems, msg)
		}
	}
	return problems
}

func checkReference(ref string, matrixKeys []string) string {
	ns, field, ok := strings.Cut(ref, ".")
	switch {
	case ok && ns == "matrix":
		for _, k := range matrixKeys {
			if k == field {
				return ""
			}
		}
		if len(matrixKeys) == 0 {
			return fmt.Sprintf("${{ %s }}: this job has no matrix", ref)
		}
		return fmt.Sprintf("${{ %s }}: unknown matrix key %q (have: %s)", ref, field, strings.Join(matrixKeys, ", "))
	case ok && ns == "event":
		for _, f := range eventFields {
			if f == field {
				return ""
			}
		}
		return fmt.Sprintf("${{ %s }}: unknown event field %q (have: %s)", ref, field, strings.Join(eventFields, ", "))
	}
	return fmt.Sprintf("${{ %s }}: expected matrix.<key> or event.<field>", ref)
}

// interpolate substitutes every ${{ }} in s. References were validated at
// parse time, so unknown ones are left untouched.
func interpolate(s string, vars map[string]any) string {
	if !strings.Contains(s, "${{") {
		return s
	}
	return interpRe.ReplaceAllStringFunc(s, func(m string) string {
		ref := interpRe.FindStringSubmatch(m)[1]
		ns, field, _ := strings.Cut(ref, ".")
		switch v := vars[ns].(type) {
		case map[string]string:
			if val, ok := v[field]; ok {
				return val
			}
		case map[string]any:
			if val, ok := v[field]; ok {
				switch val := val.(type) {
				case string:
					return val
				case bool:
					return strconv.FormatBool(val)
				}
			}
		}
		return m
	})
}

func interpolateMap(m map[string]string, vars map[string]any) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = interpolate(v, vars)
	}
	return out
}

func interpolateList(l []string, vars map[string]any) []string {
	if l == nil {
		return nil
	}
	out := make([]string, len(l))
	for i, v := range l {
		out[i] = interpolate(v, vars)
	}
	return out
}
