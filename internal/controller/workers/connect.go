package workers

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
)

// Defaults for ConnectConfig.
const (
	DefaultHelloTimeout     = 10 * time.Second
	DefaultHeartbeatTimeout = 30 * time.Second
	DefaultTouchInterval    = 30 * time.Second
)

// ConnectConfig tunes the Connect stream. Zero values use the defaults.
type ConnectConfig struct {
	// HelloTimeout is how long a new stream may take to send Hello.
	HelloTimeout time.Duration
	// HeartbeatTimeout ends a session that has sent nothing for this long.
	// Workers send heartbeats every 10s by default.
	HeartbeatTimeout time.Duration
	// TouchInterval is how often last_seen is written to the database and
	// revocation is checked.
	TouchInterval time.Duration
}

func (c ConnectConfig) withDefaults() ConnectConfig {
	if c.HelloTimeout <= 0 {
		c.HelloTimeout = DefaultHelloTimeout
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.TouchInterval <= 0 {
		c.TouchInterval = DefaultTouchInterval
	}
	return c
}

// Stream is the server side of a Connect stream.
type Stream interface {
	Receive() (*opificav1.ConnectRequest, error)
	Send(*opificav1.ConnectResponse) error
}

// MessageHandler receives what workers send about jobs. It is nil until the
// scheduler exists; until then those messages are logged and dropped.
type MessageHandler interface {
	HandleWorkerMessage(ctx context.Context, worker uuid.UUID, msg *opificav1.ConnectRequest) error
}

type peerCertKey struct{}

// WithPeerCert stores the verified TLS client certificate, if any, in the
// request context so RPC handlers can identify the worker.
func WithPeerCert(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
			r = r.WithContext(context.WithValue(r.Context(), peerCertKey{}, r.TLS.VerifiedChains[0][0]))
		}
		h.ServeHTTP(w, r)
	})
}

// PeerCert returns the verified client certificate stored by WithPeerCert.
func PeerCert(ctx context.Context) *x509.Certificate {
	c, _ := ctx.Value(peerCertKey{}).(*x509.Certificate)
	return c
}

var (
	errNoCert     = connect.NewError(connect.CodeUnauthenticated, errors.New("a worker client certificate is required"))
	errRevoked    = connect.NewError(connect.CodePermissionDenied, errors.New("worker has been revoked"))
	errUnknown    = connect.NewError(connect.CodeUnauthenticated, errors.New("unknown worker"))
	errSuperseded = connect.NewError(connect.CodeUnauthenticated, errors.New("certificate has been superseded"))

	errRevokedCause = errors.New("revoked")
)

// Connect runs one worker's stream until it ends. The worker is identified
// by its client certificate, never by what it says in Hello.
func (s *Service) Connect(ctx context.Context, cert *x509.Certificate, stream Stream) error {
	cfg := s.StreamConfig.withDefaults()
	if cert == nil {
		return errNoCert
	}
	idStr, err := ca.WorkerID(cert)
	if err != nil {
		return errNoCert
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return errNoCert
	}
	w, err := s.Store.GetWorker(ctx, id)
	switch {
	case store.IsNotFound(err):
		return errUnknown
	case err != nil:
		s.log().Error("loading worker", "worker", id, "err", err)
		return connect.NewError(connect.CodeUnavailable, errors.New("try again later"))
	case w.RevokedAt != nil:
		return errRevoked
	case w.CertSerial != cert.SerialNumber.Text(16):
		return errSuperseded
	}

	// Deferred first so it runs last: the writer must be done with the stream
	// before the handler returns.
	var writer sync.WaitGroup
	defer writer.Wait()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// Receive in the background so timeouts and cancellation can interrupt
	// waiting for the next message.
	msgs := make(chan *opificav1.ConnectRequest)
	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Receive()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case msgs <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	hello, err := s.awaitHello(ctx, msgs, recvErr, cfg.HelloTimeout)
	if err != nil {
		return err
	}
	if hello.GetWorkerId() != id.String() {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("hello names worker %q but the certificate is for %s", hello.GetWorkerId(), id))
	}
	info := hello.GetInfo()
	maxJobs := int(info.GetMaxConcurrentJobs())
	if maxJobs < 1 || maxJobs > maxJobsPerHost {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("max_concurrent_jobs must be between 1 and %d", maxJobsPerHost))
	}
	if err := s.Store.UpdateWorkerHello(ctx, db.UpdateWorkerHelloParams{
		ID: id, Hostname: info.GetHostname(), Version: info.GetVersion(), MaxConcurrentJobs: int32(maxJobs),
	}); err != nil {
		s.log().Error("recording hello", "worker", id, "err", err)
		return connect.NewError(connect.CodeUnavailable, errors.New("try again later"))
	}

	var labels map[string]string
	if err := json.Unmarshal(w.Labels, &labels); err != nil {
		return connect.NewError(connect.CodeInternal, errors.New("stored worker labels are invalid"))
	}
	now := s.now()
	sess := &Session{
		WorkerID: id, Pool: w.Pool, OS: w.Os, Arch: w.Arch, Labels: labels, MaxJobs: maxJobs,
		ConnectedAt: now, lastSeen: now, running: hello.GetRunningJobIds(),
		out: make(chan *opificav1.ConnectResponse, 16), ctx: ctx, cancel: cancel,
	}
	s.Sessions().register(sess)
	defer s.Sessions().unregister(sess)
	log := s.log().With("worker", id, "hostname", info.GetHostname())
	log.Info("worker connected", "pool", w.Pool, "os", w.Os, "arch", w.Arch, "max_jobs", maxJobs)

	// Writer: the only goroutine that sends on the stream.
	writer.Add(1)
	go func() {
		defer writer.Done()
		for {
			select {
			case m := <-sess.out:
				if err := stream.Send(m); err != nil {
					cancel(err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	idle := time.NewTimer(cfg.HeartbeatTimeout)
	defer idle.Stop()
	touch := time.NewTicker(cfg.TouchInterval)
	defer touch.Stop()

	for {
		select {
		case m := <-msgs:
			idle.Reset(cfg.HeartbeatTimeout)
			sess.seen(s.now())
			if err := s.handle(ctx, sess, m); err != nil {
				log.Info("worker disconnected", "reason", err)
				return err
			}
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				log.Info("worker disconnected", "reason", "closed by worker")
				return nil
			}
			log.Info("worker disconnected", "reason", err)
			return err
		case <-idle.C:
			log.Warn("worker disconnected", "reason", "no heartbeat")
			return connect.NewError(connect.CodeDeadlineExceeded,
				fmt.Errorf("no message for %v", cfg.HeartbeatTimeout))
		case <-touch.C:
			revoked, err := s.Store.TouchWorker(ctx, id)
			if err != nil && ctx.Err() == nil {
				log.Error("recording heartbeat", "err", err)
				continue
			}
			if revoked != nil {
				log.Warn("worker disconnected", "reason", "revoked")
				return errRevoked
			}
		case <-ctx.Done():
			cause := context.Cause(ctx)
			log.Info("worker disconnected", "reason", cause)
			if errors.Is(cause, errReplaced) {
				return connect.NewError(connect.CodeAborted, errReplaced)
			}
			if errors.Is(cause, errRevokedCause) {
				return errRevoked
			}
			return connect.NewError(connect.CodeCanceled, cause)
		}
	}
}

func (s *Service) awaitHello(ctx context.Context, msgs <-chan *opificav1.ConnectRequest, recvErr <-chan error, timeout time.Duration) (*opificav1.Hello, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case m := <-msgs:
		h := m.GetHello()
		if h == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("the first message must be hello"))
		}
		return h, nil
	case err := <-recvErr:
		return nil, err
	case <-t.C:
		return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("no hello received"))
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *Service) handle(ctx context.Context, sess *Session, m *opificav1.ConnectRequest) error {
	switch msg := m.GetMessage().(type) {
	case *opificav1.ConnectRequest_Heartbeat:
		sess.heartbeat(msg.Heartbeat, s.now())
		return nil
	case *opificav1.ConnectRequest_Hello:
		return connect.NewError(connect.CodeInvalidArgument, errors.New("hello may only be sent once"))
	case *opificav1.ConnectRequest_JobUpdate, *opificav1.ConnectRequest_LogChunk, *opificav1.ConnectRequest_SecretRequest:
		if s.Messages == nil {
			s.log().Debug("dropping job message: no scheduler yet", "worker", sess.WorkerID)
			return nil
		}
		return s.Messages.HandleWorkerMessage(ctx, sess.WorkerID, m)
	case nil:
		return connect.NewError(connect.CodeInvalidArgument, errors.New("empty message"))
	}
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown message %T", m.GetMessage()))
}

// Revoke marks a worker as revoked and disconnects it if this controller
// holds its session. Other controllers disconnect it at their next touch.
func (s *Service) Revoke(ctx context.Context, id uuid.UUID) error {
	if _, err := s.Store.RevokeWorker(ctx, id); err != nil {
		if store.IsNotFound(err) {
			return fmt.Errorf("worker %s not found or already revoked", id)
		}
		return err
	}
	s.Sessions().Disconnect(id, errRevokedCause)
	return nil
}
