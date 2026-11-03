// Package schema embeds the JSON Schema for pipeline files, used by editors
// for autocompletion and validation.
package schema

import _ "embed"

// Pipeline is the JSON Schema for .opifica/pipeline.yaml.
//
//go:embed pipeline.schema.json
var Pipeline []byte
