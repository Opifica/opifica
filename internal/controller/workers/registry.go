package workers

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
)

// ErrNotConnected is returned when sending to a worker without a live session.
var ErrNotConnected = errors.New("worker is not connected")

// Session is one live Connect stream from a worker.
type Session struct {
	WorkerID uuid.UUID
	Pool     string
	OS       string
	Arch     string
	Labels   map[string]string
	// Images are the golden image names the worker can boot.
	Images      []string
	MaxJobs     int
	ConnectedAt time.Time

	out    chan *opificav1.ConnectResponse
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu       sync.Mutex
	lastSeen time.Time
	warmVMs  uint32
	running  []string
}

// Send queues a message for the worker. It fails if the session ends first.
func (s *Session) Send(ctx context.Context, msg *opificav1.ConnectResponse) error {
	select {
	case s.out <- msg:
		return nil
	case <-s.ctx.Done():
		return ErrNotConnected
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) heartbeat(hb *opificav1.Heartbeat, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSeen = now
	s.warmVMs = hb.GetWarmVms()
	s.running = slices.Clone(hb.GetRunningJobIds())
}

func (s *Session) seen(now time.Time) {
	s.mu.Lock()
	s.lastSeen = now
	s.mu.Unlock()
}

// SessionInfo is a snapshot of a session, for the scheduler and admin views.
type SessionInfo struct {
	WorkerID    uuid.UUID
	Pool        string
	OS          string
	Arch        string
	Labels      map[string]string
	Images      []string
	MaxJobs     int
	ConnectedAt time.Time
	LastSeen    time.Time
	WarmVMs     uint32
	RunningJobs []string
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionInfo{
		WorkerID: s.WorkerID, Pool: s.Pool, OS: s.OS, Arch: s.Arch,
		Labels: maps.Clone(s.Labels), Images: slices.Clone(s.Images), MaxJobs: s.MaxJobs, ConnectedAt: s.ConnectedAt,
		LastSeen: s.lastSeen, WarmVMs: s.warmVMs, RunningJobs: slices.Clone(s.running),
	}
}

// Registry holds the live session of every connected worker.
type Registry struct {
	mu       sync.Mutex
	sessions map[uuid.UUID]*Session
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{sessions: map[uuid.UUID]*Session{}}
}

// errReplaced ends a session when the same worker connects again, e.g. after
// a network change left the old stream half-open.
var errReplaced = errors.New("replaced by a newer connection from the same worker")

func (r *Registry) register(s *Session) {
	r.mu.Lock()
	old := r.sessions[s.WorkerID]
	r.sessions[s.WorkerID] = s
	r.mu.Unlock()
	if old != nil {
		old.cancel(errReplaced)
	}
}

// unregister removes s and reports whether it was still the worker's
// current session (false if a newer connection replaced it).
func (r *Registry) unregister(s *Session) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[s.WorkerID] == s {
		delete(r.sessions, s.WorkerID)
		return true
	}
	return false
}

// Get returns the live session for a worker.
func (r *Registry) Get(id uuid.UUID) (*Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	return s, ok
}

// Send queues a message for a connected worker.
func (r *Registry) Send(ctx context.Context, id uuid.UUID, msg *opificav1.ConnectResponse) error {
	s, ok := r.Get(id)
	if !ok {
		return ErrNotConnected
	}
	return s.Send(ctx, msg)
}

// Disconnect ends a worker's session, if it has one.
func (r *Registry) Disconnect(id uuid.UUID, cause error) {
	if s, ok := r.Get(id); ok {
		s.cancel(cause)
	}
}

// List returns a snapshot of every connected worker, ordered by ID.
func (r *Registry) List() []SessionInfo {
	r.mu.Lock()
	ss := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		ss = append(ss, s)
	}
	r.mu.Unlock()
	out := make([]SessionInfo, len(ss))
	for i, s := range ss {
		out[i] = s.info()
	}
	slices.SortFunc(out, func(a, b SessionInfo) int { return compareUUID(a.WorkerID, b.WorkerID) })
	return out
}

func compareUUID(a, b uuid.UUID) int {
	for i := range a {
		if a[i] != b[i] {
			return int(a[i]) - int(b[i])
		}
	}
	return 0
}
