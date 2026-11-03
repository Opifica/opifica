// Package logs stores job output on the controller's disk, one file per job.
//
// Workers resend chunks after a reconnect; chunks are numbered per job, and
// anything not newer than the last stored chunk is dropped. The last number
// is kept in memory, so after a controller restart a resent chunk may appear
// twice. S3-compatible storage is planned (docs/ROADMAP.md, M7).
package logs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// Store appends and reads job logs.
type Store struct {
	Dir string

	mu   sync.Mutex
	open map[uuid.UUID]*jobLog
}

type jobLog struct {
	mu      sync.Mutex
	f       *os.File
	lastSeq uint64
}

// ErrNoLog means a job has no output yet.
var ErrNoLog = errors.New("no log for this job yet")

func (s *Store) path(job uuid.UUID) string {
	return filepath.Join(s.Dir, job.String()+".log")
}

func (s *Store) get(job uuid.UUID) (*jobLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open == nil {
		s.open = map[uuid.UUID]*jobLog{}
	}
	if l, ok := s.open[job]; ok {
		return l, nil
	}
	if err := os.MkdirAll(s.Dir, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.path(job), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	l := &jobLog{f: f}
	s.open[job] = l
	return l, nil
}

// Append stores chunk seq of a job's log. It reports whether the chunk was
// new.
func (s *Store) Append(job uuid.UUID, seq uint64, data []byte) (bool, error) {
	l, err := s.get(job)
	if err != nil {
		return false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq <= l.lastSeq {
		return false, nil
	}
	if _, err := l.f.Write(data); err != nil {
		return false, fmt.Errorf("writing log: %w", err)
	}
	l.lastSeq = seq
	return true, nil
}

// Finish closes a job's log file once the job is done.
func (s *Store) Finish(job uuid.UUID) {
	s.mu.Lock()
	l, ok := s.open[job]
	delete(s.open, job)
	s.mu.Unlock()
	if ok {
		l.mu.Lock()
		l.f.Close()
		l.mu.Unlock()
	}
}

// Open returns a job's log.
func (s *Store) Open(job uuid.UUID) (io.ReadCloser, error) {
	f, err := os.Open(s.path(job))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoLog
	}
	return f, err
}

// ReadFrom returns up to max bytes of a job's log starting at offset, and
// the offset to continue from.
func (s *Store) ReadFrom(job uuid.UUID, offset int64, max int) ([]byte, int64, error) {
	f, err := os.Open(s.path(job))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	buf := make([]byte, max)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, offset, err
	}
	return buf[:n], offset + int64(n), nil
}
