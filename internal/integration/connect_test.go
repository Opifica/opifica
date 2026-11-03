// Package integration runs the controller and the worker kit against each
// other over real TLS connections and a real PostgreSQL database.
package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/api"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/testutil/pgtest"
	"github.com/opifica/opifica/pkg/worker"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func quiet() *slog.Logger {
	if os.Getenv("OPIFICA_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type env struct {
	t     *testing.T
	svc   *workers.Service
	sched *scheduler.Scheduler
	url   string
}

// newEnv starts a controller with short timeouts on a random port.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	st, err := store.Open(ctx, pgtest.New(t))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := &workers.Service{
		Store: st, CA: authority, Log: quiet(),
		StreamConfig: workers.ConnectConfig{
			HelloTimeout:     2 * time.Second,
			HeartbeatTimeout: 500 * time.Millisecond,
			TouchInterval:    50 * time.Millisecond,
		},
	}
	sched := scheduler.New(st, svc.Sessions(), quiet(), scheduler.Config{
		Lease: 400 * time.Millisecond, Interval: 20 * time.Millisecond,
	})
	svc.Messages = sched
	go sched.Run(ctx)
	tlsCfg, err := api.TLSConfig(authority, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		server.ServeTLS(ctx, ln, api.NewHandler(svc), tlsCfg, quiet())
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		st.Close()
	})
	return &env{t: t, svc: svc, sched: sched, url: "https://" + ln.Addr().String()}
}

func info() *opificav1.WorkerInfo {
	return &opificav1.WorkerInfo{
		Hostname: "build-1", Os: opificav1.OperatingSystem_OPERATING_SYSTEM_LINUX,
		Arch: opificav1.Architecture_ARCHITECTURE_AMD64, MaxConcurrentJobs: 4, Version: "test",
		Labels: map[string]string{"kvm": "true"},
		Images: []*opificav1.ImageRef{{Name: "default"}},
	}
}

// join creates a token for pool and joins a worker with it.
func (e *env) join(pool string) *worker.Identity {
	e.t.Helper()
	return e.joinWith(pool, info())
}

func (e *env) joinWith(pool string, wi *opificav1.WorkerInfo) *worker.Identity {
	e.t.Helper()
	tok, err := e.svc.CreateToken(context.Background(), workers.TokenRequest{Pool: pool})
	if err != nil {
		e.t.Fatal(err)
	}
	id, err := worker.Join(context.Background(), worker.JoinConfig{
		ControllerURL: e.url, Token: tok.Secret, CAFingerprint: tok.CAFingerprint,
		StateDir: e.t.TempDir(), Info: wi,
	})
	if err != nil {
		e.t.Fatalf("Join: %v", err)
	}
	return id
}

type runningClient struct {
	*worker.Client
	cancel context.CancelFunc
	done   chan error
	msgs   chan *opificav1.ConnectResponse
}

func (e *env) start(id *worker.Identity, mutate func(*worker.ClientConfig)) *runningClient {
	e.t.Helper()
	rc := &runningClient{done: make(chan error, 1), msgs: make(chan *opificav1.ConnectResponse, 16)}
	cfg := worker.ClientConfig{
		ControllerURL: e.url, Identity: id, Info: info(),
		HeartbeatInterval: 50 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Logger: quiet(),
		Status:    func() (uint32, []string) { return 3, []string{"job-1"} },
		OnMessage: func(_ context.Context, m *opificav1.ConnectResponse) { rc.msgs <- m },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := worker.NewClient(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	rc.Client = c
	ctx, cancel := context.WithCancel(context.Background())
	rc.cancel = cancel
	go func() { rc.done <- c.Run(ctx) }()
	e.t.Cleanup(func() {
		cancel()
		<-rc.done
	})
	return rc
}

func workerHTTPClient(id *worker.Identity) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: id.TLSConfig(), ForceAttemptHTTP2: true}}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (e *env) session(id *worker.Identity) (workers.SessionInfo, bool) {
	for _, s := range e.svc.Sessions().List() {
		if s.WorkerID.String() == id.WorkerID {
			return s, true
		}
	}
	return workers.SessionInfo{}, false
}

func TestWorkerConnectsAndReportsStatus(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	e.start(id, nil)

	eventually(t, "a session with heartbeat status", func() bool {
		s, ok := e.session(id)
		return ok && s.WarmVMs == 3 && len(s.RunningJobs) == 1
	})
	s, _ := e.session(id)
	if s.Pool != "trusted" || s.OS != "linux" || s.Arch != "amd64" || s.MaxJobs != 4 || s.Labels["kvm"] != "true" {
		t.Errorf("session = %+v", s)
	}

	eventually(t, "last_seen recorded", func() bool {
		w, err := e.svc.Store.GetWorker(context.Background(), uuid.MustParse(id.WorkerID))
		return err == nil && w.LastSeenAt != nil && w.Version == "test"
	})
}

func TestControllerSendsToWorker(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	rc := e.start(id, nil)
	eventually(t, "session", func() bool { _, ok := e.session(id); return ok })

	err := e.svc.Sessions().Send(context.Background(), uuid.MustParse(id.WorkerID), &opificav1.ConnectResponse{
		Message: &opificav1.ConnectResponse_Drain{Drain: &opificav1.Drain{Reason: "maintenance"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-rc.msgs:
		if m.GetDrain().GetReason() != "maintenance" {
			t.Errorf("worker got %v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker never received the message")
	}

	if err := e.svc.Sessions().Send(context.Background(), uuid.New(), &opificav1.ConnectResponse{}); !errors.Is(err, workers.ErrNotConnected) {
		t.Errorf("send to unknown worker: err = %v", err)
	}
}

func TestWorkerReconnects(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	e.start(id, nil)
	eventually(t, "session", func() bool { _, ok := e.session(id); return ok })
	first, _ := e.session(id)

	e.svc.Sessions().Disconnect(first.WorkerID, errors.New("network blip"))
	eventually(t, "a new session", func() bool {
		s, ok := e.session(id)
		return ok && s.ConnectedAt.After(first.ConnectedAt)
	})
}

func TestSilentWorkerIsDropped(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	// Heartbeats far apart: the controller gives up after 500ms of silence.
	e.start(id, func(c *worker.ClientConfig) { c.HeartbeatInterval = time.Hour })
	eventually(t, "session", func() bool { _, ok := e.session(id); return ok })
	first, _ := e.session(id)

	// The worker reconnects, is dropped again, and so on: each time a new session.
	eventually(t, "the silent session to be replaced", func() bool {
		s, ok := e.session(id)
		return ok && s.ConnectedAt.After(first.ConnectedAt)
	})
}

func TestRevokedWorkerIsDisconnectedAndStaysOut(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	rc := e.start(id, nil)
	eventually(t, "session", func() bool { _, ok := e.session(id); return ok })

	if err := e.svc.Revoke(context.Background(), uuid.MustParse(id.WorkerID)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-rc.done:
		if connect.CodeOf(err) != connect.CodePermissionDenied || !worker.IsPermanent(err) {
			t.Errorf("Run returned %v, want permission denied", err)
		}
		rc.done <- err // for the cleanup
	case <-time.After(5 * time.Second):
		t.Fatal("revoked worker is still running")
	}
	if _, ok := e.session(id); ok {
		t.Error("revoked worker still has a session")
	}

	// Revoked on another controller (directly in the database): the
	// periodic touch finds it.
	id2 := e.join("trusted")
	rc2 := e.start(id2, nil)
	eventually(t, "second session", func() bool { _, ok := e.session(id2); return ok })
	if _, err := e.svc.Store.RevokeWorker(context.Background(), uuid.MustParse(id2.WorkerID)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-rc2.done:
		if !worker.IsPermanent(err) {
			t.Errorf("Run returned %v, want a permanent error", err)
		}
		rc2.done <- err
	case <-time.After(5 * time.Second):
		t.Fatal("worker revoked in the database is still connected")
	}
}

func TestConnectRejects(t *testing.T) {
	e := newEnv(t)
	id := e.join("trusted")
	ctx := context.Background()

	connectWith := func(t *testing.T, hello *opificav1.Hello) error {
		t.Helper()
		client := opificav1connect.NewWorkerServiceClient(workerHTTPClient(id), e.url)
		stream := client.Connect(ctx)
		defer stream.CloseResponse()
		stream.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_Hello{Hello: hello}})
		_, err := stream.Receive()
		return err
	}

	t.Run("hello for another worker", func(t *testing.T) {
		err := connectWith(t, &opificav1.Hello{WorkerId: uuid.NewString(), Info: info()})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("err = %v, want invalid argument", err)
		}
	})
	t.Run("bad capacity", func(t *testing.T) {
		bad := info()
		bad.MaxConcurrentJobs = 0
		err := connectWith(t, &opificav1.Hello{WorkerId: id.WorkerID, Info: bad})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("err = %v, want invalid argument", err)
		}
	})
	t.Run("superseded certificate", func(t *testing.T) {
		if _, err := e.svc.Store.Pool.Exec(ctx, "UPDATE workers SET cert_serial = 'other' WHERE id = $1", id.WorkerID); err != nil {
			t.Fatal(err)
		}
		err := connectWith(t, &opificav1.Hello{WorkerId: id.WorkerID, Info: info()})
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("err = %v, want unauthenticated", err)
		}
	})
}

// A worker whose certificate is from a different CA can't connect.
func TestForeignCertificateRejected(t *testing.T) {
	e := newEnv(t)
	other := newEnv(t)
	foreign := other.join("trusted")

	c, err := worker.NewClient(worker.ClientConfig{
		ControllerURL: e.url, Identity: foreign, Info: info(), Logger: quiet(), MaxBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Neither side trusts the other, so the TLS handshake fails on every
	// attempt and Run keeps retrying until the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run returned %v, want it to keep retrying until the deadline", err)
	}
	if n := len(e.svc.Sessions().List()); n != 0 {
		t.Errorf("%d sessions, want 0", n)
	}
}

func TestManyWorkers(t *testing.T) {
	e := newEnv(t)
	var wg sync.WaitGroup
	ids := make([]*worker.Identity, 10)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids[i] = e.join("untrusted")
		}()
	}
	wg.Wait()
	for _, id := range ids {
		e.start(id, nil)
	}
	eventually(t, "10 sessions", func() bool { return len(e.svc.Sessions().List()) == 10 })
}
