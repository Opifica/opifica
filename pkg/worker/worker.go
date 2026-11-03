package worker

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/version"
)

// Config is everything an OS-specific worker passes to Run.
type Config struct {
	ControllerURL string
	// StateDir holds the identity from Join and the repository mirrors.
	StateDir   string
	Hypervisor Hypervisor
	// Pool sizes the VM pool. Its Capacity is how many jobs run at once.
	Pool PoolConfig
	// Labels are reported to the controller for scheduling. "pool" is set
	// by the join token and can't be chosen here.
	Labels map[string]string
	// GuestMirrorURL: see RunnerConfig.
	GuestMirrorURL func(vm VM, hostPath string) (string, error)
	// AllowLocalRepos: see Mirrors.AllowLocal. Development only.
	AllowLocalRepos bool
	// HeartbeatInterval: see ClientConfig.
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
}

// Info describes this machine for Join and Hello.
func Info(pool PoolConfig, labels map[string]string) *opificav1.WorkerInfo {
	host, _ := os.Hostname()
	info := &opificav1.WorkerInfo{
		Hostname:          host,
		Labels:            labels,
		MaxConcurrentJobs: uint32(pool.Capacity),
		Version:           version.Version,
	}
	switch runtime.GOOS {
	case "linux":
		info.Os = opificav1.OperatingSystem_OPERATING_SYSTEM_LINUX
	case "windows":
		info.Os = opificav1.OperatingSystem_OPERATING_SYSTEM_WINDOWS
	case "darwin":
		info.Os = opificav1.OperatingSystem_OPERATING_SYSTEM_MACOS
	}
	switch runtime.GOARCH {
	case "amd64":
		info.Arch = opificav1.Architecture_ARCHITECTURE_AMD64
	case "arm64":
		info.Arch = opificav1.Architecture_ARCHITECTURE_ARM64
	}
	for _, name := range slices.Sorted(maps.Keys(pool.Images)) {
		info.Images = append(info.Images, &opificav1.ImageRef{Name: name, Digest: pool.Images[name].Digest})
	}
	return info
}

// Run connects to the controller and runs jobs until ctx is cancelled or the
// controller rejects the worker for good. The worker must have joined
// (see Join) with the same StateDir.
func Run(ctx context.Context, cfg Config) error {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	id, err := LoadIdentity(cfg.StateDir)
	if err != nil {
		return err
	}
	if cfg.Hypervisor == nil {
		return errors.New("a hypervisor is required")
	}
	cfg.Pool.Logger = log
	pool, err := NewPool(cfg.Hypervisor, cfg.Pool)
	if err != nil {
		return err
	}
	defer pool.Close(context.Background())

	var outbox *Outbox
	runner := NewRunner(RunnerConfig{
		Pool:           pool,
		Mirrors:        &Mirrors{Dir: filepath.Join(cfg.StateDir, "mirrors"), AllowLocal: cfg.AllowLocalRepos},
		Send:           func(m *opificav1.ConnectRequest, onSent func()) { outbox.Send(m, onSent) },
		GuestMirrorURL: cfg.GuestMirrorURL,
		Logger:         log,
	})

	client, err := NewClient(ClientConfig{
		ControllerURL:     cfg.ControllerURL,
		Identity:          id,
		Info:              Info(cfg.Pool, cfg.Labels),
		HeartbeatInterval: cfg.HeartbeatInterval,
		Logger:            log,
		Status: func() (uint32, []string) {
			warm := 0
			for _, n := range pool.Stats().Warm {
				warm += n
			}
			return uint32(warm), runner.Running()
		},
		OnMessage: runner.HandleMessage,
	})
	if err != nil {
		return err
	}
	outbox = NewOutbox(client, 4096)

	outboxCtx, stopOutbox := context.WithCancel(context.Background())
	outboxDone := make(chan struct{})
	go func() { outbox.Run(outboxCtx); close(outboxDone) }()

	err = client.Run(ctx)

	// Stop jobs, then give their final updates a moment to go out (they
	// won't if the connection is gone; the controller's leases cover that).
	runner.Close()
	time.AfterFunc(5*time.Second, stopOutbox)
	outbox.Close()
	<-outboxDone
	stopOutbox()
	return err
}

// Outbox delivers messages to the controller in order, waiting through
// reconnects. Job updates and logs aren't lost when the connection drops
// briefly: they are sent once it is back.
type Outbox struct {
	client *Client
	ch     chan outboxItem
	closed chan struct{}
}

type outboxItem struct {
	msg    *opificav1.ConnectRequest
	onSent func()
}

// NewOutbox creates an outbox holding up to size messages. When it is full,
// Send blocks, which slows job output down instead of using unbounded memory.
func NewOutbox(c *Client, size int) *Outbox {
	return &Outbox{client: c, ch: make(chan outboxItem, size), closed: make(chan struct{})}
}

// Send queues msg. onSent, if set, runs after it was handed to the stream.
func (o *Outbox) Send(msg *opificav1.ConnectRequest, onSent func()) {
	select {
	case o.ch <- outboxItem{msg, onSent}:
	case <-o.closed:
	}
}

// Close stops accepting messages; Run delivers what is queued, then returns.
func (o *Outbox) Close() {
	select {
	case <-o.closed:
	default:
		close(o.closed)
	}
}

// Run sends queued messages until Close was called and the queue is empty,
// or ctx is cancelled.
func (o *Outbox) Run(ctx context.Context) {
	for {
		var it outboxItem
		select {
		case it = <-o.ch:
		case <-o.closed:
			select {
			case it = <-o.ch:
			default:
				return
			}
		case <-ctx.Done():
			return
		}
		for {
			select {
			case <-o.client.Connected():
			case <-ctx.Done():
				return
			}
			if err := o.client.Send(it.msg); err == nil {
				break
			}
			select { // the stream just ended; wait for the next one
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
		if it.onSent != nil {
			it.onSent()
		}
	}
}
