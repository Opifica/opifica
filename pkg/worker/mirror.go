package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
)

// Mirrors keeps a bare mirror of each repository on the worker's disk, so
// jobs check out from a local copy: fast, light on the forge, and the VM
// never needs access to the forge itself.
type Mirrors struct {
	// Dir holds the mirrors.
	Dir string
	// AllowLocal permits local paths and file:// URLs as clone URLs. Only for
	// development: a controller could otherwise make the worker read any
	// repository on its disk.
	AllowLocal bool

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// ErrCommitNotFound means the repository doesn't contain the commit.
var ErrCommitNotFound = errors.New("commit not found in the repository")

var commitRe = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

func (m *Mirrors) lock(key string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks == nil {
		m.locks = map[string]*sync.Mutex{}
	}
	l, ok := m.locks[key]
	if !ok {
		l = &sync.Mutex{}
		m.locks[key] = l
	}
	return l
}

func (m *Mirrors) checkURL(u string) error {
	switch {
	case strings.HasPrefix(u, "https://"), strings.HasPrefix(u, "ssh://"):
		return nil
	case regexp.MustCompile(`^[\w.-]+@[\w.-]+:`).MatchString(u): // git@host:org/repo
		return nil
	case strings.HasPrefix(u, "http://"):
		return fmt.Errorf("clone URL %q: plain http is not allowed", u)
	case m.AllowLocal:
		return nil
	}
	return fmt.Errorf("clone URL %q: local repositories are not allowed on this worker", u)
}

// Ensure makes sure the mirror of cloneURL contains sha and returns the
// mirror's path. A commit already in the mirror is used without fetching:
// commits never change.
func (m *Mirrors) Ensure(ctx context.Context, cloneURL string, cred *opificav1.Credential, sha string) (string, error) {
	if !commitRe.MatchString(sha) {
		return "", fmt.Errorf("invalid commit id %q", sha)
	}
	if err := m.checkURL(cloneURL); err != nil {
		return "", err
	}
	env, cleanup, err := credentialEnv(cred)
	if err != nil {
		return "", err
	}
	defer cleanup()

	sum := sha256.Sum256([]byte(cloneURL))
	key := hex.EncodeToString(sum[:12])
	path := filepath.Join(m.Dir, key+".git")
	l := m.lock(key)
	l.Lock()
	defer l.Unlock()

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(m.Dir, 0o700); err != nil {
			return "", err
		}
		tmp, err := os.MkdirTemp(m.Dir, key+".tmp-")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(tmp)
		if _, err := runGit(ctx, env, "clone", "--quiet", "--mirror", "--", cloneURL, tmp); err != nil {
			return "", err
		}
		if err := os.Rename(tmp, path); err != nil {
			return "", err
		}
	}

	if hasCommit(ctx, path, sha) {
		return path, nil
	}
	if _, err := runGit(ctx, env, "-C", path, "fetch", "--quiet", "--prune", "origin"); err != nil {
		return "", err
	}
	if hasCommit(ctx, path, sha) {
		return path, nil
	}
	// Not on any branch or tag (e.g. a pull request ref): ask for it directly.
	if _, err := runGit(ctx, env, "-C", path, "fetch", "--quiet", "origin", sha); err == nil && hasCommit(ctx, path, sha) {
		return path, nil
	}
	return "", fmt.Errorf("%w: %s", ErrCommitNotFound, sha)
}

func hasCommit(ctx context.Context, repo, sha string) bool {
	_, err := runGit(ctx, nil, "-C", repo, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

// credentialEnv passes a token to git through the environment rather than
// the command line, where other users of the host could see it.
func credentialEnv(cred *opificav1.Credential) ([]string, func(), error) {
	noop := func() {}
	switch k := cred.GetKind().(type) {
	case nil:
		return nil, noop, nil
	case *opificav1.Credential_Token:
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + k.Token))
		return []string{
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: Basic " + basic,
		}, noop, nil
	case *opificav1.Credential_SshPrivateKey:
		return nil, noop, errors.New("SSH key credentials are not supported yet")
	}
	return nil, noop, fmt.Errorf("unknown credential type %T", cred.GetKind())
}

func runGit(ctx context.Context, env []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	cmd.Env = append(cmd.Env, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %v: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
