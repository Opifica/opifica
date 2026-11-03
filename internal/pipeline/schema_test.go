package pipeline

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

const schemaPath = "../../schema/pipeline.schema.json"

func loadSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	sch, err := c.Compile(schemaPath)
	if err != nil {
		t.Fatalf("compiling schema: %v", err)
	}
	return sch
}

// yamlToJSONValue converts YAML into the value types the validator expects.
func yamlToJSONValue(t *testing.T, src []byte) any {
	t.Helper()
	var v any
	if err := yaml.Unmarshal(src, &v); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	out, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	return out
}

// The schema powers editor autocompletion, so it must accept every file the
// parser accepts.
func TestSchemaAcceptsExamples(t *testing.T) {
	sch := loadSchema(t)
	files, _ := filepath.Glob("testdata/valid/*.yaml")
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if err := sch.Validate(yamlToJSONValue(t, data)); err != nil {
				t.Errorf("schema rejects a valid pipeline:\n%v", err)
			}
		})
	}
	// The shorthands from TestShorthands and TestParseFull, too.
	for _, src := range []string{
		"on: [push, tag]\njobs:\n  build:\n    runs-on: linux\n    steps:\n      - run: make\n",
		"on: push\njobs:\n  a:\n    runs-on: windows\n    needs: b\n    steps: [{ run: dir }]\n  b:\n    runs-on: linux\n    steps: [{ upload: dist/* }, { sign: [a, b] }]\n",
	} {
		if err := sch.Validate(yamlToJSONValue(t, []byte(src))); err != nil {
			t.Errorf("schema rejects shorthand:\n%s\n%v", src, err)
		}
	}
}

func TestSchemaRejectsInvalid(t *testing.T) {
	sch := loadSchema(t)
	for name, src := range map[string]string{
		"unknown field": "on: push\njobs:\n  a:\n    runs-on: linux\n    stage: x\n    steps: [{run: x}]\n",
		"bad os":        "on: push\njobs:\n  a:\n    runs-on: solaris\n    steps: [{run: x}]\n",
		"two actions":   "on: push\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x, sign: y}]\n",
		"no steps":      "on: push\njobs:\n  a:\n    runs-on: linux\n    steps: []\n",
		"bad trigger":   "on: merge\njobs:\n  a:\n    runs-on: linux\n    steps: [{run: x}]\n",
		"bad secret":    "on: push\njobs:\n  a:\n    runs-on: linux\n    secrets: [lower]\n    steps: [{run: x}]\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := sch.Validate(yamlToJSONValue(t, []byte(src))); err == nil {
				t.Error("schema accepts an invalid pipeline")
			}
			if _, err := Parse([]byte(src)); err == nil {
				t.Error("parser accepts it too")
			}
		})
	}
}
