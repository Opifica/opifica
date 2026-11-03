package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// PoolConfig configures a Pool.
type PoolConfig struct {
	// Capacity is the most VMs that may exist at once: warm, booting and in
	// use together. On a Mac this is at most 2 (Apple's licence limit).
	Capacity int

	// Images are the golden images this worker can boot, by name. They should
	// already have passed VerifyImage.
	Images map[string]Image

	// Warm is how many booted, idle VMs to keep ready per image name.
	Warm map[string]int

	// CPUs and MemoryMB size every VM.
	CPUs     int
	MemoryMB int

	// RetryDelay is how long to wait before retrying a failed warm boot.
	// Defaults to 5s.
	RetryDelay time.Duration

	Logger *slog.Logger
}

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("pool closed")

// Pool keeps VMs booted ahead of time so a job doesn't wait for a boot, while
// never letting the number of VMs exceed Capacity.
//
// Every VM is used for exactly one job: Lease.Release always destroys it, and
// the pool boots a fresh replacement in the background.
type Pool struct {
	hv  Hypervisor
	cfg PoolConfig
	log *slog.Logger

	ctx    context.Context // cancelled by Close; used for background boots
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	warm     map[string][]VM
	booting  map[string]int // warm boots in progress, per image
	reserved int            // slots held by Acquire calls and leases
	retryAt  map[string]time.Time
	changed  chan struct{} // closed and replaced whenever state changes
	closed   bool
}

// NewPool creates a pool and starts filling it.
func NewPool(hv Hypervisor, cfg PoolConfig) (*Pool, error) {
	if cfg.Capacity < 1 {
		return nil, fmt.Errorf("pool capacity must be at least 1, got %d", cfg.Capacity)
	}
	warmTotal := 0
	for name, n := range cfg.Warm {
		if _, ok := cfg.Images[name]; !ok {
			return nil, fmt.Errorf("warm pool refers to unknown image %q", name)
		}
		if n < 0 {
			return nil, fmt.Errorf("warm count for image %q is negative", name)
		}
		warmTotal += n
	}
	if warmTotal > cfg.Capacity {
		return nil, fmt.Errorf("warm VMs (%d) exceed capacity (%d)", warmTotal, cfg.Capacity)
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		hv:      hv,
		cfg:     cfg,
		log:     log.With("hypervisor", hv.Name()),
		ctx:     ctx,
		cancel:  cancel,
		warm:    map[string][]VM{},
		booting: map[string]int{},
		retryAt: map[string]time.Time{},
		changed: make(chan struct{}),
	}
	p.mu.Lock()
	p.refillLocked()
	p.mu.Unlock()
	return p, nil
}

// Lease is a VM handed to one job.
type Lease struct {
	VM    VM
	Image Image

	pool *Pool
	once sync.Once
}

// Release destroys the VM and frees its slot. Call it exactly once per lease,
// when the job is done; extra calls do nothing.
func (l *Lease) Release(ctx context.Context) error {
	var err error
	l.once.Do(func() {
		err = l.VM.Destroy(ctx)
		if err != nil {
			l.pool.log.Error("destroying VM", "vm", l.VM.ID(), "err", err)
		}
		l.pool.mu.Lock()
		l.pool.reserved--
		l.pool.notifyLocked()
		l.pool.refillLocked()
		l.pool.mu.Unlock()
	})
	return err
}

// Acquire returns a booted VM for the named image. It takes a warm VM if one
// is ready, boots one if there is free capacity, and otherwise waits. When
// the pool is full of idle VMs for other images, it destroys one of those to
// make room.
func (p *Pool) Acquire(ctx context.Context, image string) (*Lease, error) {
	img, ok := p.cfg.Images[image]
	if !ok {
		return nil, fmt.Errorf("unknown image %q", image)
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}

		// 1. A warm VM is ready.
		if vms := p.warm[image]; len(vms) > 0 {
			vm := vms[len(vms)-1]
			p.warm[image] = vms[:len(vms)-1]
			p.reserved++
			p.refillLocked()
			p.mu.Unlock()
			return &Lease{VM: vm, Image: img, pool: p}, nil
		}

		// 2. There is room to boot one now.
		if p.totalLocked() < p.cfg.Capacity {
			p.reserved++
			p.mu.Unlock()
			return p.bootForLease(ctx, img, nil)
		}

		// 3. Full, but idle VMs of another image hold the space.
		if victim := p.evictLocked(image); victim != nil {
			p.reserved++
			p.mu.Unlock()
			return p.bootForLease(ctx, img, victim)
		}

		// 4. Wait for something to change.
		ch := p.changed
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// bootForLease boots a VM in a slot the caller already reserved. If victim is
// set, it is destroyed first so the number of live VMs never exceeds capacity.
func (p *Pool) bootForLease(ctx context.Context, img Image, victim VM) (*Lease, error) {
	if victim != nil {
		if err := victim.Destroy(ctx); err != nil {
			p.log.Error("destroying evicted VM", "vm", victim.ID(), "err", err)
		}
	}
	vm, err := p.boot(ctx, img)
	if err != nil {
		p.mu.Lock()
		p.reserved--
		p.notifyLocked()
		p.refillLocked()
		p.mu.Unlock()
		return nil, err
	}
	return &Lease{VM: vm, Image: img, pool: p}, nil
}

func (p *Pool) boot(ctx context.Context, img Image) (VM, error) {
	vm, err := p.hv.Create(ctx, VMSpec{Image: img, CPUs: p.cfg.CPUs, MemoryMB: p.cfg.MemoryMB})
	if err != nil {
		return nil, fmt.Errorf("creating VM from image %q: %w", img.Name, err)
	}
	if err := vm.Start(ctx); err != nil {
		if derr := vm.Destroy(context.WithoutCancel(ctx)); derr != nil {
			p.log.Error("destroying VM that failed to start", "vm", vm.ID(), "err", derr)
		}
		return nil, fmt.Errorf("starting VM from image %q: %w", img.Name, err)
	}
	return vm, nil
}

// evictLocked removes and returns an idle VM of an image other than keep,
// preferring images with the most idle VMs.
func (p *Pool) evictLocked(keep string) VM {
	best := ""
	for name, vms := range p.warm {
		if name != keep && len(vms) > 0 && (best == "" || len(vms) > len(p.warm[best])) {
			best = name
		}
	}
	if best == "" {
		return nil
	}
	vms := p.warm[best]
	vm := vms[len(vms)-1]
	p.warm[best] = vms[:len(vms)-1]
	return vm
}

func (p *Pool) totalLocked() int {
	n := p.reserved
	for _, vms := range p.warm {
		n += len(vms)
	}
	for _, b := range p.booting {
		n += b
	}
	return n
}

func (p *Pool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// refillLocked starts background boots until every image has its warm count,
// as far as capacity allows.
func (p *Pool) refillLocked() {
	if p.closed {
		return
	}
	now := time.Now()
	for name, want := range p.cfg.Warm {
		if now.Before(p.retryAt[name]) {
			continue
		}
		for len(p.warm[name])+p.booting[name] < want && p.totalLocked() < p.cfg.Capacity {
			p.booting[name]++
			p.wg.Add(1)
			go p.warmBoot(name)
		}
	}
}

func (p *Pool) warmBoot(name string) {
	defer p.wg.Done()
	vm, err := p.boot(p.ctx, p.cfg.Images[name])

	p.mu.Lock()
	defer p.mu.Unlock()
	p.booting[name]--
	defer p.notifyLocked()

	if err != nil {
		if p.closed {
			return
		}
		p.log.Error("warm boot failed", "image", name, "err", err, "retry_in", p.cfg.RetryDelay)
		p.retryAt[name] = time.Now().Add(p.cfg.RetryDelay)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			select {
			case <-time.After(p.cfg.RetryDelay):
				p.mu.Lock()
				p.refillLocked()
				p.mu.Unlock()
			case <-p.ctx.Done():
			}
		}()
		return
	}
	if p.closed {
		// Close ran while this VM was booting.
		if err := vm.Destroy(context.Background()); err != nil {
			p.log.Error("destroying VM after close", "vm", vm.ID(), "err", err)
		}
		return
	}
	p.warm[name] = append(p.warm[name], vm)
}

// Stats is a snapshot of the pool.
type Stats struct {
	Warm     map[string]int
	Booting  int
	InUse    int
	Capacity int
}

// Stats returns a snapshot of the pool, e.g. for heartbeats.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Stats{Warm: map[string]int{}, InUse: p.reserved, Capacity: p.cfg.Capacity}
	for name, vms := range p.warm {
		s.Warm[name] = len(vms)
	}
	for _, b := range p.booting {
		s.Booting += b
	}
	return s
}

// Close stops refilling, destroys every idle VM and waits for background
// boots to finish. VMs out on lease are not touched: release them first.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	var idle []VM
	for name, vms := range p.warm {
		idle = append(idle, vms...)
		delete(p.warm, name)
	}
	p.notifyLocked()
	p.mu.Unlock()

	p.cancel()
	var errs []error
	for _, vm := range idle {
		if err := vm.Destroy(ctx); err != nil {
			errs = append(errs, fmt.Errorf("destroying VM %s: %w", vm.ID(), err))
		}
	}
	p.wg.Wait()
	return errors.Join(errs...)
}
