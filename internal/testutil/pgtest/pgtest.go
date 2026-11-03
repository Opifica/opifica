// Package pgtest gives tests a fresh PostgreSQL database each.
//
// If OPIFICA_TEST_DATABASE_URL is set (as in CI), databases are created on
// that server. Otherwise a throwaway server is started from a local
// PostgreSQL installation (initdb and pg_ctl on PATH or under
// /usr/lib/postgresql/*/bin). If neither is available, tests are skipped,
// unless OPIFICA_TEST_REQUIRE_DB is set: then they fail, so CI can't pass by
// skipping.
//
// Use it from TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	envURL     = "OPIFICA_TEST_DATABASE_URL"
	envRequire = "OPIFICA_TEST_REQUIRE_DB"
)

var (
	mu         sync.Mutex
	adminURL   string // server to create test databases on
	skipReason string
	stop       func()
)

// Run starts the server if needed, runs the tests and stops it again.
func Run(m *testing.M) int {
	setup()
	code := m.Run()
	if stop != nil {
		stop()
	}
	return code
}

func setup() {
	if u := os.Getenv(envURL); u != "" {
		adminURL = u
		return
	}
	bin, err := findBinaries()
	if err != nil {
		skipReason = fmt.Sprintf("no database: set %s or install PostgreSQL (%v)", envURL, err)
		return
	}
	u, s, err := startServer(bin)
	if err != nil {
		skipReason = fmt.Sprintf("starting a temporary PostgreSQL: %v", err)
		return
	}
	adminURL, stop = u, s
}

// New returns the URL of a new, empty database, dropped when the test ends.
func New(t testing.TB) string {
	t.Helper()
	mu.Lock()
	base, reason := adminURL, skipReason
	mu.Unlock()
	if base == "" {
		if reason == "" {
			reason = "pgtest.Run was not called from TestMain"
		}
		if os.Getenv(envRequire) != "" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}

	b := make([]byte, 6)
	rand.Read(b)
	name := "opifica_test_" + hex.EncodeToString(b)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connecting to test server: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("creating test database: %v", err)
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		defer conn.Close(ctx)
		conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parsing %s: %v", envURL, err)
	}
	u.Path = "/" + name
	return u.String()
}

func findBinaries() (string, error) {
	if p, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(p), nil
	}
	dirs, _ := filepath.Glob("/usr/lib/postgresql/*/bin")
	sort.Strings(dirs)
	for i := len(dirs) - 1; i >= 0; i-- { // newest version first
		if _, err := os.Stat(filepath.Join(dirs[i], "initdb")); err == nil {
			return dirs[i], nil
		}
	}
	return "", fmt.Errorf("initdb not found")
}

// startServer runs a private server listening only on a Unix socket in a
// temporary directory.
func startServer(bin string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "opifica-pgtest-")
	if err != nil {
		return "", nil, err
	}
	data := filepath.Join(dir, "data")
	cleanup := func() { os.RemoveAll(dir) }

	out, err := exec.Command(filepath.Join(bin, "initdb"), "-D", data, "-U", "postgres",
		"--auth=trust", "--encoding=UTF8", "--no-sync").CombinedOutput()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("initdb: %v: %s", err, out)
	}
	opts := fmt.Sprintf("-k %s -c listen_addresses='' -c fsync=off -c synchronous_commit=off -c full_page_writes=off", dir)
	out, err = exec.Command(filepath.Join(bin, "pg_ctl"), "-D", data, "-o", opts,
		"-l", filepath.Join(dir, "log"), "-w", "-t", "30", "start").CombinedOutput()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("pg_ctl start: %v: %s", err, out)
	}
	stopFn := func() {
		exec.Command(filepath.Join(bin, "pg_ctl"), "-D", data, "-m", "immediate", "stop").Run()
		cleanup()
	}

	u := "postgres://postgres@/postgres?host=" + url.QueryEscape(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, u)
	if err != nil {
		stopFn()
		return "", nil, err
	}
	conn.Close(ctx)
	return u, stopFn, nil
}
