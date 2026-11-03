// Package mask replaces secret values in streamed output.
//
// Output arrives in arbitrary chunks, so a secret can be split across two of
// them. Writer holds back the tail of each chunk that could be the start of
// a secret until the next chunk (or Flush) shows whether it is.
package mask

import (
	"bytes"
	"io"
	"sort"
	"sync"
)

// Replacement is written in place of every secret value.
const Replacement = "***"

// minLen is the shortest value that is masked. Shorter values ("1", "true")
// would mangle ordinary output while protecting nothing.
const minLen = 4

// Writer masks secrets in everything written to it before passing it on.
type Writer struct {
	mu      sync.Mutex
	out     io.Writer
	secrets [][]byte // longest first, so overlapping secrets mask fully
	maxLen  int
	pending []byte
}

// NewWriter returns a Writer that masks secrets in what it writes to out.
// Values shorter than 4 bytes are not masked.
func NewWriter(out io.Writer, secrets []string) *Writer {
	w := &Writer{out: out}
	seen := map[string]bool{}
	for _, s := range secrets {
		if len(s) < minLen || seen[s] {
			continue
		}
		seen[s] = true
		w.secrets = append(w.secrets, []byte(s))
		w.maxLen = max(w.maxLen, len(s))
	}
	sort.Slice(w.secrets, func(i, j int) bool { return len(w.secrets[i]) > len(w.secrets[j]) })
	return w
}

// Write masks and forwards p, except for a tail that might be the start of a
// secret, which waits for the next Write or Flush.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.secrets) == 0 {
		return len(p), w.write(p)
	}
	buf := append(w.pending, p...)
	masked, keep := w.mask(buf, false)
	w.pending = append(w.pending[:0:0], buf[len(buf)-keep:]...)
	return len(p), w.write(masked)
}

// Flush writes whatever is being held back. Call it when the stream ends.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return nil
	}
	masked, _ := w.mask(w.pending, true)
	w.pending = nil
	return w.write(masked)
}

func (w *Writer) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	_, err := w.out.Write(p)
	return err
}

// mask returns buf with secrets replaced, and how many bytes at the end of
// buf were held back (always 0 when final).
func (w *Writer) mask(buf []byte, final bool) ([]byte, int) {
	var out bytes.Buffer
	i := 0
	for i < len(buf) {
		if n := w.matchAt(buf[i:]); n > 0 {
			out.WriteString(Replacement)
			i += n
			continue
		}
		if !final && w.couldStartSecret(buf[i:]) {
			return out.Bytes(), len(buf) - i
		}
		out.WriteByte(buf[i])
		i++
	}
	return out.Bytes(), 0
}

// matchAt returns the length of the secret that b starts with, or 0.
func (w *Writer) matchAt(b []byte) int {
	for _, s := range w.secrets {
		if bytes.HasPrefix(b, s) {
			return len(s)
		}
	}
	return 0
}

// couldStartSecret reports whether b (the rest of the buffer) is a proper
// prefix of some secret, i.e. more input could complete it.
func (w *Writer) couldStartSecret(b []byte) bool {
	if len(b) >= w.maxLen {
		return false
	}
	for _, s := range w.secrets {
		if len(b) < len(s) && bytes.HasPrefix(s, b) {
			return true
		}
	}
	return false
}

// String masks secrets in a complete string.
func String(s string, secrets []string) string {
	var b bytes.Buffer
	w := NewWriter(&b, secrets)
	w.Write([]byte(s))
	w.Flush()
	return b.String()
}
