package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/opifica/opifica/pkg/worker"
	"github.com/opifica/opifica/pkg/worker/workertest"
)

var images = map[string]worker.Image{
	"default":  {Name: "default"},
	"xcode-16": {Name: "xcode-16"},
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newPool(t *testing.T, hv worker.Hypervisor, capacity int, warm map[string]int) *worker.Pool {
	t.Helper()
	p, err := worker.NewPool(hv, worker.PoolConfig{
		Capacity: capacity, Images: images, Warm: warm,
		RetryDelay: 20 * time.Millisecond, Logger: quiet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	return p
}

// eventually polls cond until it holds or the test times out.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPoolFillsWarm(t *testing.T) {
	hv := &workertest.Hypervisor{}
	p := newPool(t, hv, 4, map[string]int{"default": 2, "xcode-16": 1})
	eventually(t, "warm pool to fill", func() bool {
		s := p.Stats()
		return s.Warm["default"] == 2 && s.Warm["xcode-16"] == 1
	})
	if hv.Alive() != 3 {
		t.Errorf("alive = %d, want 3", hv.Alive())
	}
}

func TestAcquireUsesWarmVMAndReplacesIt(t *testing.T) {
	hv := &workertest.Hypervisor{BootDelay: 50 * time.Millisecond}
	p := newPool(t, hv, 2, map[string]int{"default": 1})
	eventually(t, "warm VM", func() bool { return p.Stats().Warm["default"] == 1 })

	start := time.Now()
	l, err := p.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 25*time.Millisecond {
		t.Errorf("acquiring a warm VM took %v; it should not wait for a boot", d)
	}
	eventually(t, "replacement VM", func() bool { return p.Stats().Warm["default"] == 1 })
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseAlwaysDestroys(t *testing.T) {
	hv := &workertest.Hypervisor{}
	p := newPool(t, hv, 1, nil)
	seen := map[string]bool{}
	for range 5 {
		l, err := p.Acquire(context.Background(), "default")
		if err != nil {
			t.Fatal(err)
		}
		if seen[l.VM.ID()] {
			t.Fatalf("VM %s was handed out twice", l.VM.ID())
		}
		seen[l.VM.ID()] = true
		l.Release(context.Background())
		l.Release(context.Background()) // extra calls are harmless
	}
	if created, destroyed := hv.Counts(); created != 5 || destroyed != 5 {
		t.Errorf("created=%d destroyed=%d, want 5 and 5", created, destroyed)
	}
}

func TestAcquireWaitsWhenFull(t *testing.T) {
	hv := &workertest.Hypervisor{}
	p := newPool(t, hv, 1, nil)
	l1, err := p.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, "default"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire on a full pool: err = %v, want deadline exceeded", err)
	}

	got := make(chan *worker.Lease)
	go func() {
		l, err := p.Acquire(context.Background(), "default")
		if err != nil {
			t.Error(err)
		}
		got <- l
	}()
	time.Sleep(10 * time.Millisecond)
	l1.Release(context.Background())
	select {
	case l2 := <-got:
		l2.Release(context.Background())
	case <-time.After(5 * time.Second):
		t.Fatal("waiting acquire never got a VM after release")
	}
}

// A Mac with capacity 2 kept warm for "default" must still serve an
// "xcode-16" job, by destroying an idle VM first: never by booting a third.
func TestEvictsIdleVMOfOtherImage(t *testing.T) {
	hv := &workertest.Hypervisor{}
	p := newPool(t, hv, 2, map[string]int{"default": 2})
	eventually(t, "warm pool to fill", func() bool { return p.Stats().Warm["default"] == 2 })

	l, err := p.Acquire(context.Background(), "xcode-16")
	if err != nil {
		t.Fatal(err)
	}
	if l.Image.Name != "xcode-16" {
		t.Errorf("image = %q", l.Image.Name)
	}
	if hv.MaxAlive() > 2 {
		t.Errorf("max alive = %d, capacity is 2", hv.MaxAlive())
	}
	l.Release(context.Background())
	eventually(t, "warm pool to refill", func() bool { return p.Stats().Warm["default"] == 2 })
}

func TestCapacityNeverExceeded(t *testing.T) {
	hv := &workertest.Hypervisor{BootDelay: time.Millisecond}
	const capacity = 2
	p := newPool(t, hv, capacity, map[string]int{"default": 1, "xcode-16": 1})

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			img := "default"
			if i%3 == 0 {
				img = "xcode-16"
			}
			for range 10 {
				l, err := p.Acquire(context.Background(), img)
				if err != nil {
					t.Error(err)
					return
				}
				time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond)
				l.Release(context.Background())
			}
		}()
	}
	wg.Wait()
	if m := hv.MaxAlive(); m > capacity {
		t.Errorf("max alive VMs = %d, capacity is %d", m, capacity)
	}
	if created, _ := hv.Counts(); created < 200 {
		t.Errorf("created %d VMs for 200 jobs; every job needs a fresh VM", created)
	}
}

func TestBootFailure(t *testing.T) {
	hv := &workertest.Hypervisor{FailStart: func(worker.VMSpec) error { return errors.New("no kvm") }}
	p := newPool(t, hv, 1, nil)
	if _, err := p.Acquire(context.Background(), "default"); err == nil {
		t.Fatal("acquire succeeded with a failing hypervisor")
	}
	if s := p.Stats(); s.InUse != 0 {
		t.Errorf("failed boot still holds a slot: %+v", s)
	}
	if hv.Alive() != 0 {
		t.Errorf("failed VM was not destroyed")
	}
}

func TestWarmBootRetries(t *testing.T) {
	var mu sync.Mutex
	fails := 3
	hv := &workertest.Hypervisor{FailStart: func(worker.VMSpec) error {
		mu.Lock()
		defer mu.Unlock()
		if fails > 0 {
			fails--
			return errors.New("transient")
		}
		return nil
	}}
	p := newPool(t, hv, 1, map[string]int{"default": 1})
	eventually(t, "warm VM after retries", func() bool { return p.Stats().Warm["default"] == 1 })
}

func TestCloseDestroysIdleVMs(t *testing.T) {
	hv := &workertest.Hypervisor{BootDelay: 5 * time.Millisecond}
	p, err := worker.NewPool(hv, worker.PoolConfig{Capacity: 3, Images: images, Warm: map[string]int{"default": 3}, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond) // close while some VMs are still booting
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hv.Alive() != 0 {
		t.Errorf("%d VMs still alive after close", hv.Alive())
	}
	if _, err := p.Acquire(context.Background(), "default"); !errors.Is(err, worker.ErrPoolClosed) {
		t.Errorf("acquire after close: err = %v", err)
	}
}

func TestPoolConfigValidation(t *testing.T) {
	hv := &workertest.Hypervisor{}
	for name, cfg := range map[string]worker.PoolConfig{
		"zero capacity":   {Capacity: 0, Images: images},
		"unknown image":   {Capacity: 1, Images: images, Warm: map[string]int{"ubuntu": 1}},
		"warm > capacity": {Capacity: 2, Images: images, Warm: map[string]int{"default": 2, "xcode-16": 1}},
	} {
		if _, err := worker.NewPool(hv, cfg); err == nil {
			t.Errorf("%s: NewPool accepted an invalid config", name)
		}
	}
	p := newPool(t, hv, 1, nil)
	if _, err := p.Acquire(context.Background(), "ubuntu"); err == nil {
		t.Error("acquire accepted an unknown image")
	}
}

func TestVerifyImage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.img")
	if err := os.WriteFile(path, []byte("golden image"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := worker.FileDigest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.VerifyImage(worker.Image{Name: "default", Path: path, Digest: digest}); err != nil {
		t.Errorf("valid image rejected: %v", err)
	}

	if err := os.WriteFile(path, []byte("tampered image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := worker.VerifyImage(worker.Image{Name: "default", Path: path, Digest: digest}); !errors.Is(err, worker.ErrDigestMismatch) {
		t.Errorf("tampered image: err = %v, want ErrDigestMismatch", err)
	}
	if err := worker.VerifyImage(worker.Image{Name: "default", Path: path, Digest: "md5:abc"}); err == nil {
		t.Error("malformed digest accepted")
	}
}
