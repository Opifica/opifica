// Package agent is the program that runs inside every job VM. It serves the
// AgentService to the worker on the host: checking out code, running steps
// and collecting files. It never talks to the controller, and it only ever
// sees the secrets of the step it is running.
package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/mask"
	"github.com/opifica/opifica/internal/version"
)

// TimeoutExitCode is reported when a command is stopped because it ran out
// of time, matching the timeout(1) convention.
const TimeoutExitCode = 124

const (
	chunkSize    = 64 << 10
	maxCollected = 2 << 30 // per Collect call
)

// Server implements opificav1connect.AgentServiceHandler.
type Server struct {
	// Root is the work directory. Every path in a request is relative to it
	// and may not leave it.
	Root string
	Log  *slog.Logger
}

var _ opificav1connect.AgentServiceHandler = (*Server)(nil)

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// resolve turns a request path into a path under Root.
func (s *Server) resolve(p string) (string, error) {
	if p == "" {
		return s.Root, nil
	}
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) {
		return "", fmt.Errorf("path %q must be relative to the work directory", p)
	}
	full := filepath.Join(s.Root, p)
	rel, err := filepath.Rel(s.Root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q leaves the work directory", p)
	}
	return full, nil
}

func invalid(err error) error { return connect.NewError(connect.CodeInvalidArgument, err) }

// Ready implements AgentServiceHandler.
func (s *Server) Ready(context.Context, *connect.Request[opificav1.ReadyRequest]) (*connect.Response[opificav1.ReadyResponse], error) {
	return connect.NewResponse(&opificav1.ReadyResponse{
		AgentVersion: version.Version, Os: runtime.GOOS, Arch: runtime.GOARCH,
	}), nil
}

var shaRe = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// Checkout implements AgentServiceHandler.
func (s *Server) Checkout(ctx context.Context, req *connect.Request[opificav1.CheckoutRequest]) (*connect.Response[opificav1.CheckoutResponse], error) {
	m := req.Msg
	if !shaRe.MatchString(m.GetCommitSha()) {
		return nil, invalid(fmt.Errorf("commit_sha must be a full hex commit id, got %q", m.GetCommitSha()))
	}
	if m.GetMirrorUrl() == "" {
		return nil, invalid(errors.New("mirror_url is required"))
	}
	dir, err := s.resolve(m.GetDirectory())
	if err != nil {
		return nil, invalid(err)
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return nil, invalid(fmt.Errorf("checkout directory %q is not empty", m.GetDirectory()))
	}

	steps := [][]string{
		{"clone", "--quiet", "--no-checkout", "--", m.GetMirrorUrl(), dir},
		{"-C", dir, "checkout", "--quiet", "--detach", m.GetCommitSha()},
		{"-C", dir, "rev-parse", "HEAD"},
	}
	var head string
	for _, args := range steps {
		out, err := git(ctx, args...)
		if err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		head = strings.TrimSpace(out)
	}
	if head != m.GetCommitSha() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("checked out %s, wanted %s", head, m.GetCommitSha()))
	}
	return connect.NewResponse(&opificav1.CheckoutResponse{HeadSha: head}), nil
}

func git(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	// Never prompt, never read the host's or user's git config.
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %v: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// shellCommand returns the program and arguments that run script.
func shellCommand(shell, script string) ([]string, error) {
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "pwsh"
			if _, err := exec.LookPath("pwsh"); err != nil {
				shell = "powershell"
			}
		} else {
			shell = "bash"
			if _, err := exec.LookPath("bash"); err != nil {
				shell = "sh"
			}
		}
	}
	switch shell {
	case "bash":
		// Stop at the first failing command, including inside pipelines.
		return []string{"bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script}, nil
	case "sh":
		return []string{"sh", "-e", "-c", script}, nil
	case "pwsh", "powershell":
		return []string{shell, "-NoProfile", "-NonInteractive", "-Command",
			"$ErrorActionPreference = 'Stop'\n" + script + "\nif ($LASTEXITCODE) { exit $LASTEXITCODE }"}, nil
	case "cmd":
		return []string{"cmd", "/D", "/S", "/C", script}, nil
	}
	return nil, fmt.Errorf("unknown shell %q (bash, sh, pwsh, powershell or cmd)", shell)
}

// streamWriter sends output as ExecResponse chunks. stdout and stderr write
// from different goroutines, so sends are serialized.
type streamWriter struct {
	mu     *sync.Mutex
	stream *connect.ServerStream[opificav1.ExecResponse]
	stderr bool
}

func (w *streamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	data := bytes.Clone(p)
	msg := &opificav1.ExecResponse{Event: &opificav1.ExecResponse_Stdout{Stdout: data}}
	if w.stderr {
		msg.Event = &opificav1.ExecResponse_Stderr{Stderr: data}
	}
	if err := w.stream.Send(msg); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Exec implements AgentServiceHandler. Output is masked before it leaves the
// agent. The command and everything it started are killed when the step
// times out or the worker cancels the call.
func (s *Server) Exec(ctx context.Context, req *connect.Request[opificav1.ExecRequest], stream *connect.ServerStream[opificav1.ExecResponse]) error {
	m := req.Msg
	if strings.TrimSpace(m.GetCommand()) == "" {
		return invalid(errors.New("command is required"))
	}
	argv, err := shellCommand(m.GetShell(), m.GetCommand())
	if err != nil {
		return invalid(err)
	}
	dir, err := s.resolve(m.GetWorkingDirectory())
	if err != nil {
		return invalid(err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return invalid(fmt.Errorf("working directory %q does not exist", m.GetWorkingDirectory()))
	}

	runCtx := ctx
	var cancelTimeout context.CancelFunc = func() {}
	if t := m.GetTimeoutSeconds(); t > 0 {
		runCtx, cancelTimeout = context.WithTimeout(ctx, time.Duration(t)*time.Second)
	}
	defer cancelTimeout()

	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	for k, v := range m.GetEnv() {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	killTree(cmd)
	// If a background process keeps the output open, don't wait forever.
	cmd.WaitDelay = 5 * time.Second

	var mu sync.Mutex
	stdout := mask.NewWriter(&streamWriter{mu: &mu, stream: stream}, m.GetMask())
	stderr := mask.NewWriter(&streamWriter{mu: &mu, stream: stream, stderr: true}, m.GetMask())
	cmd.Stdout, cmd.Stderr = stdout, stderr

	runErr := cmd.Run()
	stdout.Flush()
	stderr.Flush()

	if ctx.Err() != nil {
		return connect.NewError(connect.CodeCanceled, errors.New("cancelled by the worker"))
	}
	code := 0
	var exitErr *exec.ExitError
	switch {
	case runErr == nil:
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		code = TimeoutExitCode
		fmt.Fprintf(stderr, "\nopifica: step timed out after %ds\n", m.GetTimeoutSeconds())
		stderr.Flush()
	case errors.As(runErr, &exitErr):
		code = exitErr.ExitCode()
		if code < 0 {
			code = 1 // killed by a signal
		}
	default:
		// The command couldn't start, e.g. the shell is missing.
		return connect.NewError(connect.CodeFailedPrecondition, runErr)
	}
	return stream.Send(&opificav1.ExecResponse{Event: &opificav1.ExecResponse_ExitCode{ExitCode: int32(code)}})
}

// Collect implements AgentServiceHandler. Patterns are globs relative to
// base_directory. Symbolic links are skipped, so a job can't make the agent
// read files outside the work directory.
func (s *Server) Collect(ctx context.Context, req *connect.Request[opificav1.CollectRequest], stream *connect.ServerStream[opificav1.CollectResponse]) error {
	base, err := s.resolve(req.Msg.GetBaseDirectory())
	if err != nil {
		return invalid(err)
	}
	seen := map[string]bool{}
	var total int64
	for _, pattern := range req.Msg.GetPatterns() {
		if filepath.IsAbs(pattern) || strings.Contains(filepath.ToSlash(pattern), "..") {
			return invalid(fmt.Errorf("pattern %q must stay inside the base directory", pattern))
		}
		matches, err := filepath.Glob(filepath.Join(base, pattern))
		if err != nil {
			return invalid(fmt.Errorf("pattern %q: %v", pattern, err))
		}
		for _, path := range matches {
			if seen[path] {
				continue
			}
			seen[path] = true
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() {
				continue // directories, symlinks, devices
			}
			if total += info.Size(); total > maxCollected {
				return connect.NewError(connect.CodeResourceExhausted, fmt.Errorf("collected files exceed %d bytes", maxCollected))
			}
			rel, _ := filepath.Rel(base, path)
			if err := sendFile(ctx, stream, path, filepath.ToSlash(rel)); err != nil {
				return err
			}
		}
	}
	return nil
}

func sendFile(ctx context.Context, stream *connect.ServerStream[opificav1.CollectResponse], path, rel string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	buf := make([]byte, chunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := f.Read(buf)
		eof := errors.Is(err, io.EOF)
		if err != nil && !eof {
			return err
		}
		if n > 0 || eof {
			if err := stream.Send(&opificav1.CollectResponse{Path: rel, Data: bytes.Clone(buf[:n]), Eof: eof}); err != nil {
				return err
			}
		}
		if eof {
			return nil
		}
	}
}
