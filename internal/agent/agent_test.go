package agent_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/agent"
	"github.com/opifica/opifica/internal/server"
)

func start(t *testing.T) (opificav1connect.AgentServiceClient, string) {
	t.Helper()
	root := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, ln, agent.Handler(&agent.Server{Root: root, Log: log}), log)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	addr := ln.Addr().String()
	client := agent.NewClient(func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	})
	return client, root
}

type result struct {
	stdout, stderr string
	exit           int32
	err            error
}

func run(t *testing.T, c opificav1connect.AgentServiceClient, req *opificav1.ExecRequest) result {
	t.Helper()
	return runCtx(context.Background(), t, c, req)
}

func runCtx(ctx context.Context, t *testing.T, c opificav1connect.AgentServiceClient, req *opificav1.ExecRequest) result {
	t.Helper()
	stream, err := c.Exec(ctx, connect.NewRequest(req))
	if err != nil {
		return result{err: err}
	}
	var r result
	r.exit = -1
	for stream.Receive() {
		switch e := stream.Msg().GetEvent().(type) {
		case *opificav1.ExecResponse_Stdout:
			r.stdout += string(e.Stdout)
		case *opificav1.ExecResponse_Stderr:
			r.stderr += string(e.Stderr)
		case *opificav1.ExecResponse_ExitCode:
			r.exit = e.ExitCode
		}
	}
	r.err = stream.Err()
	return r
}

func unixOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX shell")
	}
}

func TestReady(t *testing.T) {
	c, _ := start(t)
	resp, err := c.Ready(context.Background(), connect.NewRequest(&opificav1.ReadyRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetOs() != runtime.GOOS || resp.Msg.GetArch() != runtime.GOARCH {
		t.Errorf("ready = %v", resp.Msg)
	}
}

func TestExec(t *testing.T) {
	unixOnly(t)
	c, root := start(t)
	os.MkdirAll(filepath.Join(root, "src", "sub"), 0o755)

	r := run(t, c, &opificav1.ExecRequest{
		Command:          `echo "out $GREETING"; echo err >&2; pwd`,
		WorkingDirectory: "src/sub",
		Env:              map[string]string{"GREETING": "hello"},
	})
	if r.err != nil || r.exit != 0 {
		t.Fatalf("exit %d, err %v", r.exit, r.err)
	}
	if !strings.HasPrefix(r.stdout, "out hello\n") || r.stderr != "err\n" {
		t.Errorf("stdout %q, stderr %q", r.stdout, r.stderr)
	}
	if !strings.HasSuffix(strings.TrimSpace(r.stdout), filepath.Join("src", "sub")) {
		t.Errorf("ran in the wrong directory: %q", r.stdout)
	}

	if r := run(t, c, &opificav1.ExecRequest{Command: "exit 3"}); r.exit != 3 || r.err != nil {
		t.Errorf("exit 3: got exit %d, err %v", r.exit, r.err)
	}
}

func TestBashStopsAtFirstFailure(t *testing.T) {
	unixOnly(t)
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("no bash")
	}
	c, _ := start(t)
	r := run(t, c, &opificav1.ExecRequest{Command: "false | true\necho should-not-run"})
	if r.exit == 0 || strings.Contains(r.stdout, "should-not-run") {
		t.Errorf("a failing pipeline didn't stop the step: exit %d, stdout %q", r.exit, r.stdout)
	}
	r = run(t, c, &opificav1.ExecRequest{Command: "echo ok", Shell: "sh"})
	if r.exit != 0 || r.stdout != "ok\n" {
		t.Errorf("sh: exit %d, stdout %q", r.exit, r.stdout)
	}
}

func TestSecretsAreMasked(t *testing.T) {
	unixOnly(t)
	c, _ := start(t)
	// printf in pieces so the secret reaches the agent split across writes.
	r := run(t, c, &opificav1.ExecRequest{
		Command: `printf 'token=s3cr'; sleep 0.05; printf '3t-value!\n'; echo "$TOKEN" >&2`,
		Env:     map[string]string{"TOKEN": "s3cr3t-value"},
		Mask:    []string{"s3cr3t-value"},
	})
	if strings.Contains(r.stdout+r.stderr, "s3cr3t") {
		t.Errorf("secret leaked: stdout %q, stderr %q", r.stdout, r.stderr)
	}
	if r.stdout != "token=***!\n" || r.stderr != "***\n" {
		t.Errorf("stdout %q, stderr %q", r.stdout, r.stderr)
	}
}

func TestTimeoutKillsTheWholeTree(t *testing.T) {
	unixOnly(t)
	c, root := start(t)
	pidFile := filepath.Join(root, "child.pid")
	start := time.Now()
	r := run(t, c, &opificav1.ExecRequest{
		Command:        `sleep 60 & echo $! > child.pid; wait`,
		TimeoutSeconds: 1,
	})
	if r.exit != agent.TimeoutExitCode || !strings.Contains(r.stderr, "timed out after 1s") {
		t.Errorf("exit %d, stderr %q", r.exit, r.stderr)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("took %v", d)
	}
	assertDead(t, pidFile)
}

func TestCancelKillsTheWholeTree(t *testing.T) {
	unixOnly(t)
	c, root := start(t)
	pidFile := filepath.Join(root, "child.pid")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			if _, err := os.Stat(pidFile); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	r := runCtx(ctx, t, c, &opificav1.ExecRequest{Command: `sleep 60 & echo $! > child.pid; wait`})
	if r.err == nil {
		t.Error("cancelled exec returned no error")
	}
	assertDead(t, pidFile)
}

// assertDead waits for the process whose PID is in file to be gone.
func assertDead(t *testing.T, file string) {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("reading pid: %v", err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("background process %d survived", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExecRejects(t *testing.T) {
	c, _ := start(t)
	for name, req := range map[string]*opificav1.ExecRequest{
		"empty":         {Command: " "},
		"unknown shell": {Command: "x", Shell: "fish"},
		"absolute dir":  {Command: "x", WorkingDirectory: "/etc"},
		"escaping dir":  {Command: "x", WorkingDirectory: "../.."},
		"missing dir":   {Command: "x", WorkingDirectory: "nope"},
	} {
		if r := run(t, c, req); connect.CodeOf(r.err) != connect.CodeInvalidArgument {
			t.Errorf("%s: err = %v, want invalid argument", name, r.err)
		}
	}
}

// newRepo creates a git repository with one commit and returns its path and
// the commit id.
func newRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	gitIn := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com", "GIT_CONFIG_GLOBAL="+os.DevNull)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	gitIn("init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(dir, "README"), []byte("hello\n"), 0o644)
	gitIn("add", ".")
	gitIn("commit", "-q", "-m", "first")
	first := gitIn("rev-parse", "HEAD")
	os.WriteFile(filepath.Join(dir, "README"), []byte("changed\n"), 0o644)
	gitIn("commit", "-q", "-am", "second")
	return dir, first
}

func TestCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	c, root := start(t)
	repo, first := newRepo(t)
	ctx := context.Background()

	// An older commit, not the branch head: the exact SHA is what's built.
	resp, err := c.Checkout(ctx, connect.NewRequest(&opificav1.CheckoutRequest{MirrorUrl: repo, CommitSha: first, Directory: "src"}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetHeadSha() != first {
		t.Errorf("head = %s, want %s", resp.Msg.GetHeadSha(), first)
	}
	if data, _ := os.ReadFile(filepath.Join(root, "src", "README")); string(data) != "hello\n" {
		t.Errorf("README = %q, want the first commit's", data)
	}

	for name, req := range map[string]*opificav1.CheckoutRequest{
		"not empty":    {MirrorUrl: repo, CommitSha: first, Directory: "src"},
		"short sha":    {MirrorUrl: repo, CommitSha: first[:8], Directory: "b"},
		"escaping dir": {MirrorUrl: repo, CommitSha: first, Directory: "../x"},
		"no mirror":    {CommitSha: first, Directory: "c"},
	} {
		if _, err := c.Checkout(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: err = %v, want invalid argument", name, err)
		}
	}
	missing := strings.Repeat("ab", 20)
	if _, err := c.Checkout(ctx, connect.NewRequest(&opificav1.CheckoutRequest{MirrorUrl: repo, CommitSha: missing, Directory: "d"})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("unknown commit: err = %v, want failed precondition", err)
	}
}

func TestCollect(t *testing.T) {
	c, root := start(t)
	os.MkdirAll(filepath.Join(root, "src", "build"), 0o755)
	big := strings.Repeat("x", 200<<10) // several chunks
	os.WriteFile(filepath.Join(root, "src", "build", "a.xml"), []byte("<a/>"), 0o644)
	os.WriteFile(filepath.Join(root, "src", "build", "b.xml"), []byte(big), 0o644)
	os.WriteFile(filepath.Join(root, "src", "build", "c.txt"), []byte("no"), 0o644)
	secret := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(secret, []byte("outside"), 0o644)
	os.Symlink(secret, filepath.Join(root, "src", "build", "link.xml"))

	stream, err := c.Collect(context.Background(), connect.NewRequest(&opificav1.CollectRequest{
		BaseDirectory: "src", Patterns: []string{"build/*.xml", "build/a.xml"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for stream.Receive() {
		files[stream.Msg().GetPath()] += string(stream.Msg().GetData())
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files["build/a.xml"] != "<a/>" || files["build/b.xml"] != big {
		t.Errorf("collected %d files: %v", len(files), keys(files))
	}

	bad, _ := c.Collect(context.Background(), connect.NewRequest(&opificav1.CollectRequest{Patterns: []string{"../../etc/passwd"}}))
	for bad.Receive() {
	}
	if connect.CodeOf(bad.Err()) != connect.CodeInvalidArgument {
		t.Errorf("escaping pattern: err = %v", bad.Err())
	}
}

func keys(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
