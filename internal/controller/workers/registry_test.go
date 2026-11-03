package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
)

func newSession(id uuid.UUID) *Session {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Session{WorkerID: id, out: make(chan *opificav1.ConnectResponse, 1), ctx: ctx, cancel: cancel}
}

func TestRegistryReplacesOldSession(t *testing.T) {
	r := NewRegistry()
	id := uuid.New()
	first, second := newSession(id), newSession(id)

	r.register(first)
	r.register(second)
	if !errors.Is(context.Cause(first.ctx), errReplaced) {
		t.Errorf("old session cause = %v, want errReplaced", context.Cause(first.ctx))
	}
	if second.ctx.Err() != nil {
		t.Error("new session was cancelled")
	}

	// The old session's handler unregisters on its way out; that must not
	// remove the new session.
	r.unregister(first)
	if s, ok := r.Get(id); !ok || s != second {
		t.Error("unregistering the old session removed the new one")
	}
	r.unregister(second)
	if _, ok := r.Get(id); ok {
		t.Error("session still registered")
	}
}

func TestSessionSend(t *testing.T) {
	r := NewRegistry()
	id := uuid.New()
	s := newSession(id)
	r.register(s)

	msg := &opificav1.ConnectResponse{}
	if err := r.Send(context.Background(), id, msg); err != nil {
		t.Fatal(err)
	}
	if got := <-s.out; got != msg {
		t.Error("wrong message queued")
	}

	// Queue full and the caller gives up.
	s.out <- msg
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.Send(ctx, id, msg); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("send to full queue: err = %v", err)
	}

	// Session ended while the sender waits.
	r.Disconnect(id, errors.New("gone"))
	if err := s.Send(context.Background(), msg); !errors.Is(err, ErrNotConnected) {
		t.Errorf("send to ended session: err = %v", err)
	}
}
