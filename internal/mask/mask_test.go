package mask

import (
	"bytes"
	"strings"
	"testing"
)

func TestString(t *testing.T) {
	secrets := []string{"hunter2!", "s3cr3t-token", "abc"}
	tests := map[string]string{
		"password is hunter2!":           "password is ***",
		"hunter2!hunter2!":               "******",
		"token=s3cr3t-token;":            "token=***;",
		"abc is too short to mask":       "abc is too short to mask",
		"nothing here":                   "nothing here",
		"almost hunter2 but not":         "almost hunter2 but not",
		"ends with a prefix: s3cr3t-tok": "ends with a prefix: s3cr3t-tok",
	}
	for in, want := range tests {
		if got := String(in, secrets); got != want {
			t.Errorf("String(%q) = %q, want %q", in, got, want)
		}
	}
}

// Every possible split of the input into two writes must give the same result.
func TestSecretSplitAcrossWrites(t *testing.T) {
	secrets := []string{"correct-horse-battery", "horse-battery-staple"}
	in := "a correct-horse-battery-staple b horse-battery-staple c correct-hors"
	want := String(in, secrets)
	if strings.Contains(want, "battery") && !strings.HasSuffix(want, "correct-hors") {
		t.Fatalf("baseline masking failed: %q", want)
	}
	for i := 0; i <= len(in); i++ {
		for j := i; j <= len(in); j++ {
			var b bytes.Buffer
			w := NewWriter(&b, secrets)
			w.Write([]byte(in[:i]))
			w.Write([]byte(in[i:j]))
			w.Write([]byte(in[j:]))
			w.Flush()
			if b.String() != want {
				t.Fatalf("split at %d,%d: got %q, want %q", i, j, b.String(), want)
			}
		}
	}
}

func TestByteAtATime(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b, []string{"TOPSECRET"})
	for _, c := range []byte("x TOPSECRET y TOPSECRE") {
		w.Write([]byte{c})
	}
	w.Flush()
	if got := b.String(); got != "x *** y TOPSECRE" {
		t.Errorf("got %q", got)
	}
}

func TestNoSecretsPassesThrough(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b, nil)
	w.Write([]byte("hello"))
	if b.String() != "hello" {
		t.Errorf("got %q without Flush; nothing should be held back", b.String())
	}
}
