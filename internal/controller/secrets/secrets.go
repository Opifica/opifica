// Package secrets stores secrets encrypted and hands them to jobs that the
// admin's policy allows.
//
// A job never decides what it gets. It can only ask for secrets its
// pipeline lists, with the job token of its current attempt, through the
// worker that holds it; the policy set by the admin for each secret then
// decides by repository, pool, ref and whether the run is a pull request.
// Every decision is recorded.
//
// Values are encrypted with AES-256-GCM under a key kept in a file only the
// controller can read. Moving the key into a KMS or HSM is planned
// (docs/ROADMAP.md, M5).
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	gopath "path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
)

const keyFile = "secrets.key"

// Key encrypts and decrypts secret values.
type Key struct {
	id   string
	aead cipher.AEAD
}

// LoadOrCreateKey loads the secrets key from dir, creating it on first use.
func LoadOrCreateKey(dir string) (*Key, error) {
	path := filepath.Join(dir, keyFile)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		raw = make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			return LoadOrCreateKey(dir) // another process created it first
		}
		if err != nil {
			return nil, err
		}
		if _, err := f.Write(raw); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return NewKey(raw)
}

// NewKey wraps a 32-byte AES-256 key.
func NewKey(raw []byte) (*Key, error) {
	if len(raw) != 32 {
		return nil, fmt.Errorf("secrets key must be 32 bytes, got %d", len(raw))
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return &Key{id: hex.EncodeToString(sum[:8]), aead: aead}, nil
}

// ID identifies the key without revealing it.
func (k *Key) ID() string { return k.id }

// seal encrypts value. The secret's name is bound as additional data, so a
// ciphertext copied into another secret's row won't decrypt.
func (k *Key) seal(name string, value []byte) (ciphertext, nonce []byte, err error) {
	nonce = make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return k.aead.Seal(nil, nonce, value, []byte(name)), nonce, nil
}

func (k *Key) open(name string, ciphertext, nonce []byte) ([]byte, error) {
	v, err := k.aead.Open(nil, nonce, ciphertext, []byte(name))
	if err != nil {
		return nil, errors.New("secret can't be decrypted: wrong key, or the stored value was altered")
	}
	return v, nil
}

// Policy decides which runs may use a secret.
type Policy struct {
	// Repos are repository globs. "*" alone matches every repository.
	Repos []string
	// Pools the run must be in.
	Pools []string
	// Refs are ref globs ("refs/heads/main", "refs/tags/v*"). Empty: any.
	Refs []string
	// AllowPullRequests lets pull request runs use the secret. Off by
	// default: a pull request runs code nobody has reviewed yet.
	AllowPullRequests bool
	Description       string
}

var (
	nameRe = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	poolRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
)

// MaxValueSize is the largest secret value accepted.
const MaxValueSize = 64 << 10

// Manager stores secrets and answers requests for them.
type Manager struct {
	Store *store.Store
	Key   *Key
}

// Set stores a secret, replacing any secret of the same name.
func (m *Manager) Set(ctx context.Context, name string, value []byte, p Policy) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid secret name %q (use UPPER_SNAKE_CASE)", name)
	}
	if len(value) == 0 || len(value) > MaxValueSize {
		return fmt.Errorf("secret value must be 1 to %d bytes", MaxValueSize)
	}
	if len(p.Repos) == 0 {
		return errors.New("at least one repository pattern is required (use '*' for all)")
	}
	if len(p.Pools) == 0 {
		return errors.New("at least one pool is required")
	}
	for _, pool := range p.Pools {
		if !poolRe.MatchString(pool) {
			return fmt.Errorf("invalid pool name %q", pool)
		}
	}
	for _, g := range append(slices.Clone(p.Repos), p.Refs...) {
		if _, err := match(g, ""); err != nil {
			return fmt.Errorf("invalid pattern %q: %v", g, err)
		}
	}
	ct, nonce, err := m.Key.seal(name, value)
	if err != nil {
		return err
	}
	refs := p.Refs
	if refs == nil {
		refs = []string{}
	}
	_, err = m.Store.UpsertSecret(ctx, db.UpsertSecretParams{
		Name: name, Ciphertext: ct, Nonce: nonce, KeyID: m.Key.ID(),
		Repos: p.Repos, Pools: p.Pools, Refs: refs, AllowPullRequests: p.AllowPullRequests, Description: p.Description,
	})
	return err
}

// Request asks for a secret on behalf of a job. It returns the value, or a
// reason the request was denied. Errors are for failures of the controller
// itself (e.g. the database), not denials.
func (m *Manager) Request(ctx context.Context, worker uuid.UUID, req *opificav1.SecretRequest) ([]byte, string, error) {
	var jobID, runID *uuid.UUID
	value, reason, err := m.decide(ctx, worker, req, &jobID, &runID)
	if err != nil {
		return nil, "", err
	}
	if rerr := m.Store.RecordSecretAccess(ctx, db.RecordSecretAccessParams{
		SecretName: req.GetName(), JobID: jobID, RunID: runID, WorkerID: &worker,
		Granted: reason == "", Reason: reason,
	}); rerr != nil {
		// Without an audit record, don't hand out the secret.
		return nil, "", fmt.Errorf("recording secret access: %w", rerr)
	}
	return value, reason, nil
}

func (m *Manager) decide(ctx context.Context, worker uuid.UUID, req *opificav1.SecretRequest, jobOut, runOut **uuid.UUID) ([]byte, string, error) {
	const notHeld = "the job is not running on this worker"
	jobID, err := uuid.Parse(req.GetJobId())
	if err != nil {
		return nil, notHeld, nil
	}
	job, err := m.Store.GetJob(ctx, jobID)
	if store.IsNotFound(err) {
		return nil, notHeld, nil
	}
	if err != nil {
		return nil, "", err
	}
	*jobOut, *runOut = &job.ID, &job.RunID

	// 1. The worker asking holds the job right now.
	if job.WorkerID == nil || *job.WorkerID != worker || (job.State != "assigned" && job.State != "running") {
		return nil, notHeld, nil
	}
	// 2. The token belongs to the job's current attempt.
	got := sha256.Sum256([]byte(req.GetJobToken()))
	if len(job.TokenHash) == 0 || subtle.ConstantTimeCompare(got[:], job.TokenHash) != 1 {
		return nil, "invalid job token", nil
	}
	// 3. The pipeline lists the secret for this job.
	spec := &opificav1.Job{}
	if err := proto.Unmarshal(job.Spec, spec); err != nil {
		return nil, "", fmt.Errorf("stored job spec: %w", err)
	}
	if !slices.Contains(spec.GetAllowedSecrets(), req.GetName()) {
		return nil, "the job's secrets don't include " + req.GetName(), nil
	}
	// 4. The admin's policy allows it for this run.
	sec, err := m.Store.GetSecret(ctx, req.GetName())
	if store.IsNotFound(err) {
		return nil, "no secret named " + req.GetName(), nil
	}
	if err != nil {
		return nil, "", err
	}
	run, err := m.Store.GetRun(ctx, job.RunID)
	if err != nil {
		return nil, "", err
	}
	if reason := allowed(sec, run); reason != "" {
		return nil, reason, nil
	}
	value, err := m.Key.open(sec.Name, sec.Ciphertext, sec.Nonce)
	if err != nil {
		return nil, "", err
	}
	return value, "", nil
}

// allowed returns why the policy denies sec to run, or "" if it allows it.
func allowed(sec db.Secret, run db.Run) string {
	if !matchAny(sec.Repos, run.Repo) {
		return fmt.Sprintf("not allowed for repository %s", run.Repo)
	}
	if !slices.Contains(sec.Pools, run.Pool) {
		return fmt.Sprintf("not allowed in pool %s", run.Pool)
	}
	if run.EventType == "pull_request" {
		if !sec.AllowPullRequests {
			return "not available to pull requests"
		}
	} else if len(sec.Refs) > 0 && !matchAny(sec.Refs, run.Ref) {
		return fmt.Sprintf("not allowed for %s", run.Ref)
	}
	return ""
}

func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if ok, _ := match(p, s); ok {
			return true
		}
	}
	return false
}

// match is a glob match where "*" alone matches everything, a trailing
// "/**" matches everything below a prefix, and otherwise "*" stays within
// one path segment.
func match(pattern, s string) (bool, error) {
	switch {
	case pattern == "*" || pattern == "**":
		return true, nil
	case strings.HasSuffix(pattern, "/**"):
		prefix := strings.TrimSuffix(pattern, "/**")
		if _, err := gopath.Match(prefix, ""); err != nil {
			return false, err
		}
		return s == prefix || strings.HasPrefix(s, prefix+"/"), nil
	}
	return gopath.Match(pattern, s)
}
