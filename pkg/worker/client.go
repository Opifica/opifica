package worker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
)

// Files in the worker's state directory.
const (
	idFile   = "worker.id"
	keyFile  = "worker.key"
	certFile = "worker.crt"
	caFile   = "ca.crt"
)

// Identity is what a worker keeps after joining: its ID, private key,
// certificate and the controller's CA.
type Identity struct {
	WorkerID string
	cert     tls.Certificate
	caPool   *x509.CertPool
}

// JoinConfig is what a worker needs to join a controller.
type JoinConfig struct {
	// ControllerURL is e.g. "https://controller.example:8443".
	ControllerURL string
	// Token and CAFingerprint are printed by `controller token create`.
	Token         string
	CAFingerprint string
	// StateDir is where the identity is stored. It must be private to the
	// worker: it holds the worker's private key.
	StateDir string
	Info     *opificav1.WorkerInfo
}

// Join exchanges a join token for a worker identity and saves it in
// cfg.StateDir. The private key is generated here and never leaves the
// machine; only a certificate request is sent.
func Join(ctx context.Context, cfg JoinConfig) (*Identity, error) {
	if _, err := os.Stat(filepath.Join(cfg.StateDir, idFile)); err == nil {
		return nil, fmt.Errorf("%s already holds a worker identity; remove it to join again", cfg.StateDir)
	}
	u, err := url.Parse(cfg.ControllerURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return nil, fmt.Errorf("controller URL must be https://host[:port], got %q", cfg.ControllerURL)
	}
	tlsCfg, err := PinnedCAConfig(cfg.CAFingerprint, u.Hostname())
	if err != nil {
		return nil, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, err
	}
	client := opificav1connect.NewWorkerServiceClient(httpClient(tlsCfg), cfg.ControllerURL)
	resp, err := client.Join(ctx, connect.NewRequest(&opificav1.JoinRequest{
		JoinToken: cfg.Token,
		CsrPem:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
		Info:      cfg.Info,
	}))
	if err != nil {
		return nil, fmt.Errorf("joining: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, err
	}
	// Write the ID last: its presence marks a complete identity.
	for _, f := range []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{keyFile, keyPEM, 0o600},
		{certFile, resp.Msg.GetCertificatePem(), 0o644},
		{caFile, resp.Msg.GetCaBundlePem(), 0o644},
		{idFile, []byte(resp.Msg.GetWorkerId() + "\n"), 0o644},
	} {
		if err := os.WriteFile(filepath.Join(cfg.StateDir, f.name), f.data, f.perm); err != nil {
			return nil, err
		}
	}
	return LoadIdentity(cfg.StateDir)
}

// LoadIdentity reads an identity saved by Join.
func LoadIdentity(dir string) (*Identity, error) {
	id, err := os.ReadFile(filepath.Join(dir, idFile))
	if err != nil {
		return nil, fmt.Errorf("no worker identity in %s (join first): %w", dir, err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, certFile), filepath.Join(dir, keyFile))
	if err != nil {
		return nil, fmt.Errorf("loading worker certificate: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, caFile))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("ca.crt holds no certificates")
	}
	return &Identity{WorkerID: strings.TrimSpace(string(id)), cert: cert, caPool: pool}, nil
}

// TLSConfig returns the mTLS config for talking to the controller.
func (id *Identity) TLSConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{id.cert},
		RootCAs:      id.caPool,
		MinVersion:   tls.VersionTLS13,
	}
}

func httpClient(cfg *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig:   cfg,
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   90 * time.Second,
		// Pings detect a controller that vanished without closing the
		// connection (network cut, crash), which would otherwise leave the
		// stream waiting forever.
		HTTP2: &http.HTTP2Config{SendPingTimeout: 15 * time.Second, PingTimeout: 10 * time.Second},
	}}
}

// ClientConfig configures a Client.
type ClientConfig struct {
	ControllerURL string
	Identity      *Identity
	Info          *opificav1.WorkerInfo

	// HeartbeatInterval defaults to 10s; the controller drops workers it
	// hasn't heard from for 30s.
	HeartbeatInterval time.Duration
	// Status reports what goes into heartbeats. Optional.
	Status func() (warmVMs uint32, runningJobs []string)
	// OnMessage receives every message from the controller, in order, on
	// one goroutine. It should return quickly.
	OnMessage func(ctx context.Context, msg *opificav1.ConnectResponse)
	// MaxBackoff caps the wait between reconnect attempts. Defaults to 1m.
	MaxBackoff time.Duration
	Logger     *slog.Logger
}

// Client keeps a worker connected to the controller.
type Client struct {
	cfg ClientConfig
	rpc opificav1connect.WorkerServiceClient
	log *slog.Logger

	mu     sync.Mutex
	stream *connect.BidiStreamForClient[opificav1.ConnectRequest, opificav1.ConnectResponse]
	ready  chan struct{} // closed while connected
}

// NewClient creates a client. Call Run to connect.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Identity == nil {
		return nil, errors.New("identity is required")
	}
	if cfg.Info == nil {
		return nil, errors.New("info is required")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		cfg:   cfg,
		rpc:   opificav1connect.NewWorkerServiceClient(httpClient(cfg.Identity.TLSConfig()), cfg.ControllerURL),
		log:   log.With("worker", cfg.Identity.WorkerID),
		ready: make(chan struct{}),
	}, nil
}

// ErrNotConnected is returned by Send while the client is between connections.
var ErrNotConnected = errors.New("not connected to the controller")

// Send sends a message on the current stream.
func (c *Client) Send(msg *opificav1.ConnectRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == nil {
		return ErrNotConnected
	}
	return c.stream.Send(msg)
}

// Connected returns a channel that is closed while the client is connected.
// After a disconnect a new channel is returned.
func (c *Client) Connected() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

// IsPermanent reports whether a Run error means retrying is pointless: the
// worker was revoked or its identity is no longer accepted.
func IsPermanent(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodePermissionDenied, connect.CodeUnauthenticated:
		return true
	}
	return false
}

// Run connects and stays connected, reconnecting with backoff, until ctx is
// cancelled or the controller rejects the worker permanently.
func (c *Client) Run(ctx context.Context) error {
	attempt := 0
	for {
		start := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsPermanent(err) {
			c.log.Error("controller rejected this worker", "err", err)
			return err
		}
		if time.Since(start) > c.cfg.MaxBackoff {
			attempt = 0 // the last connection was healthy
		}
		wait := backoff(attempt, c.cfg.MaxBackoff)
		attempt++
		c.log.Warn("disconnected from controller", "err", err, "retry_in", wait.Round(time.Millisecond))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// backoff is exponential from 100ms with ±25% jitter, capped at max. The
// first retry is quick because most drops are momentary, and a worker that
// is back within its lease keeps its running jobs.
func backoff(attempt int, max time.Duration) time.Duration {
	d := time.Duration(float64(100*time.Millisecond) * math.Pow(2, float64(min(attempt, 20))))
	d = min(d, max)
	return time.Duration(float64(d) * (0.75 + 0.5*mrand.Float64()))
}

func (c *Client) status() (uint32, []string) {
	if c.cfg.Status == nil {
		return 0, nil
	}
	return c.cfg.Status()
}

func (c *Client) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := c.rpc.Connect(ctx)

	_, running := c.status()
	if err := stream.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_Hello{Hello: &opificav1.Hello{
		WorkerId: c.cfg.Identity.WorkerID, Info: c.cfg.Info, RunningJobIds: running,
	}}}); err != nil {
		// The real error, e.g. a rejected certificate, comes from Receive.
		_, rerr := stream.Receive()
		return errors.Join(err, rerr)
	}

	c.mu.Lock()
	c.stream = stream
	close(c.ready)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.stream = nil
		c.ready = make(chan struct{})
		c.mu.Unlock()
		stream.CloseResponse()
	}()

	// Cancelling the context doesn't interrupt a Receive that is waiting
	// for data. Closing our side of the stream does: the controller sees
	// the end of the stream and ends its side too.
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.stream == stream {
			c.stream = nil
		}
		stream.CloseRequest()
	}()

	go func() {
		t := time.NewTicker(c.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				warm, running := c.status()
				err := c.Send(&opificav1.ConnectRequest{Message: &opificav1.ConnectRequest_Heartbeat{Heartbeat: &opificav1.Heartbeat{
					WarmVms: warm, RunningJobIds: running,
				}}})
				if err != nil {
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	c.log.Info("connected to controller")
	for {
		msg, err := stream.Receive()
		if err != nil {
			return err
		}
		if c.cfg.OnMessage != nil {
			c.cfg.OnMessage(ctx, msg)
		}
	}
}
