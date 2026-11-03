// Package workers manages build machines: join tokens, joining, and (later)
// the Connect stream and worker registry.
package workers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
)

const (
	tokenPrefix = "opj_"

	// DefaultTokenTTL is how long a join token is valid if not specified.
	DefaultTokenTTL = 24 * time.Hour
	maxTokenTTL     = 30 * 24 * time.Hour
	maxJobsPerHost  = 256
)

var (
	poolRe  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	labelRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,62}$`)
)

// reservedLabels can't be set by a worker: they come from the admin.
var reservedLabels = []string{"pool"}

// Service implements joining. It is used by the WorkerService handler and by
// admin tooling that creates tokens.
type Service struct {
	Store *store.Store
	CA    *ca.CA
	Log   *slog.Logger
	Now   func() time.Time

	// StreamConfig tunes the Connect stream.
	StreamConfig ConnectConfig
	// Messages receives job messages from workers; nil drops them.
	Messages MessageHandler

	sessionsOnce sync.Once
	sessions     *Registry
}

// Sessions returns the registry of connected workers.
func (s *Service) Sessions() *Registry {
	s.sessionsOnce.Do(func() { s.sessions = NewRegistry() })
	return s.sessions
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// TokenRequest describes a join token to create.
type TokenRequest struct {
	Pool        string
	Labels      map[string]string
	Description string
	TTL         time.Duration
}

// CreatedToken is a new join token. Secret is shown once and never stored.
type CreatedToken struct {
	ID        uuid.UUID
	Secret    string
	ExpiresAt time.Time
	// CAFingerprint lets the worker check it is talking to this controller.
	CAFingerprint string
}

// CreateToken creates a one-time join token for a worker in req.Pool.
func (s *Service) CreateToken(ctx context.Context, req TokenRequest) (*CreatedToken, error) {
	if !poolRe.MatchString(req.Pool) {
		return nil, fmt.Errorf("invalid pool name %q (lowercase letters, digits and -)", req.Pool)
	}
	if err := checkLabels(req.Labels); err != nil {
		return nil, err
	}
	ttl := req.TTL
	if ttl == 0 {
		ttl = DefaultTokenTTL
	}
	if ttl < 0 || ttl > maxTokenTTL {
		return nil, fmt.Errorf("token lifetime must be between 0 and %v", maxTokenTTL)
	}
	labels := req.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return nil, err
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	secret := tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	tok, err := s.Store.CreateJoinToken(ctx, db.CreateJoinTokenParams{
		ID:          uuid.New(),
		TokenHash:   hashToken(secret),
		Pool:        req.Pool,
		Labels:      labelsJSON,
		Description: req.Description,
		ExpiresAt:   s.now().Add(ttl),
	})
	if err != nil {
		return nil, fmt.Errorf("storing join token: %w", err)
	}
	return &CreatedToken{ID: tok.ID, Secret: secret, ExpiresAt: tok.ExpiresAt, CAFingerprint: s.CA.Fingerprint()}, nil
}

func hashToken(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// errBadToken is deliberately the same for unknown, used and expired tokens,
// so a caller learns nothing about which tokens exist.
var errBadToken = connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired join token"))

// Join admits a new worker: it consumes the join token, records the worker
// and signs its certificate request, all in one transaction.
func (s *Service) Join(ctx context.Context, req *opificav1.JoinRequest) (*opificav1.JoinResponse, error) {
	if !strings.HasPrefix(req.GetJoinToken(), tokenPrefix) {
		return nil, errBadToken
	}
	info := req.GetInfo()
	osName, arch, err := checkInfo(info)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if len(req.GetCsrPem()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("csr_pem is required"))
	}

	workerID := uuid.New()
	var certPEM []byte
	var pool string
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		tok, err := q.ClaimJoinToken(ctx, hashToken(req.GetJoinToken()))
		if store.IsNotFound(err) {
			return errBadToken
		}
		if err != nil {
			return err
		}
		pool = tok.Pool

		var tokenLabels map[string]string
		if err := json.Unmarshal(tok.Labels, &tokenLabels); err != nil {
			return fmt.Errorf("join token labels: %w", err)
		}
		// Admin-assigned labels win over what the worker reports.
		labels := maps.Clone(info.GetLabels())
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, tokenLabels)
		labelsJSON, err := json.Marshal(labels)
		if err != nil {
			return err
		}

		cert, serial, err := s.CA.SignWorkerCSR(req.GetCsrPem(), workerID.String())
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		certPEM = cert

		if _, err := q.CreateWorker(ctx, db.CreateWorkerParams{
			ID:                workerID,
			Hostname:          info.GetHostname(),
			Os:                osName,
			Arch:              arch,
			Pool:              tok.Pool,
			Labels:            labelsJSON,
			MaxConcurrentJobs: int32(info.GetMaxConcurrentJobs()),
			Version:           info.GetVersion(),
			CertSerial:        serial,
			JoinTokenID:       tok.ID,
		}); err != nil {
			return err
		}
		return q.SetJoinTokenWorker(ctx, db.SetJoinTokenWorkerParams{ID: tok.ID, UsedBy: &workerID})
	})
	if err != nil {
		var cerr *connect.Error
		if errors.As(err, &cerr) {
			return nil, cerr
		}
		s.log().Error("join failed", "err", err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("join failed"))
	}

	s.log().Info("worker joined", "worker", workerID, "hostname", info.GetHostname(), "os", osName, "arch", arch, "pool", pool)
	return &opificav1.JoinResponse{
		WorkerId:       workerID.String(),
		CertificatePem: certPEM,
		CaBundlePem:    s.CA.CertPEM(),
	}, nil
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func checkInfo(info *opificav1.WorkerInfo) (osName, arch string, err error) {
	if info == nil {
		return "", "", errors.New("info is required")
	}
	switch info.GetOs() {
	case opificav1.OperatingSystem_OPERATING_SYSTEM_LINUX:
		osName = "linux"
	case opificav1.OperatingSystem_OPERATING_SYSTEM_WINDOWS:
		osName = "windows"
	case opificav1.OperatingSystem_OPERATING_SYSTEM_MACOS:
		osName = "macos"
	default:
		return "", "", errors.New("info.os is required")
	}
	switch info.GetArch() {
	case opificav1.Architecture_ARCHITECTURE_AMD64:
		arch = "amd64"
	case opificav1.Architecture_ARCHITECTURE_ARM64:
		arch = "arm64"
	default:
		return "", "", errors.New("info.arch is required")
	}
	if h := info.GetHostname(); h == "" || len(h) > 253 {
		return "", "", errors.New("info.hostname is required (at most 253 characters)")
	}
	if n := info.GetMaxConcurrentJobs(); n < 1 || n > maxJobsPerHost {
		return "", "", fmt.Errorf("info.max_concurrent_jobs must be between 1 and %d", maxJobsPerHost)
	}
	if err := checkLabels(info.GetLabels()); err != nil {
		return "", "", err
	}
	for _, r := range reservedLabels {
		if _, ok := info.GetLabels()[r]; ok {
			return "", "", fmt.Errorf("label %q is set by the join token, not by the worker", r)
		}
	}
	return osName, arch, nil
}

func checkLabels(labels map[string]string) error {
	if len(labels) > 64 {
		return errors.New("at most 64 labels")
	}
	for k, v := range labels {
		if !labelRe.MatchString(k) {
			return fmt.Errorf("invalid label key %q", k)
		}
		if len(v) > 255 {
			return fmt.Errorf("label %q: value longer than 255 characters", k)
		}
	}
	return nil
}
