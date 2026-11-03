package logs

import (
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
)

func TestAppendDropsDuplicates(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	job := uuid.New()
	if _, err := s.Open(job); !errors.Is(err, ErrNoLog) {
		t.Errorf("Open before any output: err = %v", err)
	}
	for _, c := range []struct {
		seq  uint64
		data string
		want bool
	}{
		{1, "a", true}, {2, "b", true}, {2, "b", false}, {1, "a", false}, {3, "c", true},
	} {
		if got, err := s.Append(job, c.seq, []byte(c.data)); err != nil || got != c.want {
			t.Errorf("Append(%d) = %v, %v; want %v", c.seq, got, err, c.want)
		}
	}
	s.Finish(job)
	r, err := s.Open(job)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "abc" {
		t.Errorf("log = %q, want abc", data)
	}
}
