package store_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/testutil/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func open(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), pgtest.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	url := pgtest.New(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := store.Open(ctx, url)
			if err != nil {
				errs <- err
				return
			}
			s.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Open: %v", err)
	}

	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var n int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Error("no migrations recorded")
	}
}

func newToken(t *testing.T, s *store.Store, hash string, expires time.Time) db.JoinToken {
	t.Helper()
	tok, err := s.CreateJoinToken(context.Background(), db.CreateJoinTokenParams{
		ID: uuid.New(), TokenHash: []byte(hash), Pool: "trusted",
		Labels: json.RawMessage(`{}`), ExpiresAt: expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestJoinTokenSingleUse(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	newToken(t, s, "valid", time.Now().Add(time.Hour))
	newToken(t, s, "expired", time.Now().Add(-time.Minute))

	if _, err := s.ClaimJoinToken(ctx, []byte("valid")); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := s.ClaimJoinToken(ctx, []byte("valid")); !store.IsNotFound(err) {
		t.Errorf("second claim of the same token: err = %v, want not found", err)
	}
	if _, err := s.ClaimJoinToken(ctx, []byte("expired")); !store.IsNotFound(err) {
		t.Errorf("claim of expired token: err = %v, want not found", err)
	}
	if _, err := s.ClaimJoinToken(ctx, []byte("unknown")); !store.IsNotFound(err) {
		t.Errorf("claim of unknown token: err = %v, want not found", err)
	}
}

// Many workers racing for one token: exactly one may win.
func TestJoinTokenClaimRace(t *testing.T) {
	s := open(t)
	newToken(t, s, "contested", time.Now().Add(time.Hour))
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.ClaimJoinToken(context.Background(), []byte("contested")); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("%d claims succeeded, want exactly 1", wins)
	}
}

func TestTxRollsBack(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	tok := newToken(t, s, "tx", time.Now().Add(time.Hour))

	err := s.Tx(ctx, func(q *db.Queries) error {
		if _, err := q.ClaimJoinToken(ctx, []byte("tx")); err != nil {
			return err
		}
		return context.Canceled // anything non-nil rolls back
	})
	if err == nil {
		t.Fatal("Tx returned nil")
	}
	if _, err := s.ClaimJoinToken(ctx, tok.TokenHash); err != nil {
		t.Errorf("token was consumed despite rollback: %v", err)
	}
}
