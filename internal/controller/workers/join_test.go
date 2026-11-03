package workers_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/testutil/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func newService(t *testing.T) *workers.Service {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, pgtest.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &workers.Service{Store: st, CA: authority, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func csr(t *testing.T) []byte {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func joinRequest(t *testing.T, token string) *opificav1.JoinRequest {
	return &opificav1.JoinRequest{
		JoinToken: token,
		CsrPem:    csr(t),
		Info: &opificav1.WorkerInfo{
			Hostname:          "mac-mini-1",
			Os:                opificav1.OperatingSystem_OPERATING_SYSTEM_MACOS,
			Arch:              opificav1.Architecture_ARCHITECTURE_ARM64,
			MaxConcurrentJobs: 2,
			Labels:            map[string]string{"xcode": "16", "region": "worker-says-this"},
			Version:           "dev",
		},
	}
}

func codeOf(err error) connect.Code {
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return cerr.Code()
	}
	return 0
}

func TestJoin(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	tok, err := s.CreateToken(ctx, workers.TokenRequest{
		Pool: "release", Labels: map[string]string{"region": "eu-west"}, Description: "office Mac",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok.Secret, "opj_") || tok.CAFingerprint != s.CA.Fingerprint() {
		t.Errorf("token = %+v", tok)
	}

	resp, err := s.Join(ctx, joinRequest(t, tok.Secret))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	b, _ := pem.Decode(resp.CertificatePem)
	cert, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := ca.WorkerID(cert); id != resp.WorkerId {
		t.Errorf("certificate identity %q != worker id %q", id, resp.WorkerId)
	}
	if string(resp.CaBundlePem) != string(s.CA.CertPEM()) {
		t.Error("CA bundle missing from response")
	}

	w, err := s.Store.GetWorker(ctx, uuid.MustParse(resp.WorkerId))
	if err != nil {
		t.Fatal(err)
	}
	if w.Pool != "release" || w.Os != "macos" || w.Arch != "arm64" || w.MaxConcurrentJobs != 2 {
		t.Errorf("stored worker = %+v", w)
	}
	if w.CertSerial != cert.SerialNumber.Text(16) {
		t.Error("stored certificate serial doesn't match the issued certificate")
	}
	var labels map[string]string
	json.Unmarshal(w.Labels, &labels)
	if labels["xcode"] != "16" || labels["region"] != "eu-west" {
		t.Errorf("labels = %v; token labels must win over the worker's", labels)
	}

	// The token is spent.
	if _, err := s.Join(ctx, joinRequest(t, tok.Secret)); codeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("second join with the same token: err = %v", err)
	}
}

func TestJoinRejectsBadTokens(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	now := time.Now()
	s.Now = func() time.Time { return now.Add(-2 * time.Hour) }
	expired, err := s.CreateToken(ctx, workers.TokenRequest{Pool: "trusted", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	s.Now = nil

	for name, token := range map[string]string{
		"empty":     "",
		"no prefix": "hunter2",
		"unknown":   "opj_doesnotexist",
		"expired":   expired.Secret,
	} {
		_, err := s.Join(ctx, joinRequest(t, token))
		if codeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: err = %v, want unauthenticated", name, err)
		}
		if err != nil && !strings.Contains(err.Error(), "invalid or expired join token") {
			t.Errorf("%s: error should be the generic message, got %v", name, err)
		}
	}
}

func TestJoinRejectsBadRequests(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	tests := map[string]func(*opificav1.JoinRequest){
		"no info":          func(r *opificav1.JoinRequest) { r.Info = nil },
		"no os":            func(r *opificav1.JoinRequest) { r.Info.Os = 0 },
		"no arch":          func(r *opificav1.JoinRequest) { r.Info.Arch = 0 },
		"no hostname":      func(r *opificav1.JoinRequest) { r.Info.Hostname = "" },
		"zero jobs":        func(r *opificav1.JoinRequest) { r.Info.MaxConcurrentJobs = 0 },
		"too many jobs":    func(r *opificav1.JoinRequest) { r.Info.MaxConcurrentJobs = 10000 },
		"chooses own pool": func(r *opificav1.JoinRequest) { r.Info.Labels["pool"] = "release" },
		"bad label":        func(r *opificav1.JoinRequest) { r.Info.Labels["Bad Key"] = "x" },
		"no csr":           func(r *opificav1.JoinRequest) { r.CsrPem = nil },
		"bad csr":          func(r *opificav1.JoinRequest) { r.CsrPem = []byte("nope") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			tok, err := s.CreateToken(ctx, workers.TokenRequest{Pool: "trusted"})
			if err != nil {
				t.Fatal(err)
			}
			req := joinRequest(t, tok.Secret)
			mutate(req)
			if _, err := s.Join(ctx, req); codeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("err = %v, want invalid argument", err)
			}
			// A rejected request must not burn the token.
			if _, err := s.Join(ctx, joinRequest(t, tok.Secret)); err != nil {
				t.Errorf("token unusable after a rejected request: %v", err)
			}
		})
	}
}

// Ten machines racing with one token: exactly one becomes a worker.
func TestJoinRace(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	tok, err := s.CreateToken(ctx, workers.TokenRequest{Pool: "trusted"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	joined := 0
	for range 10 {
		req := joinRequest(t, tok.Secret)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Join(ctx, req); err == nil {
				mu.Lock()
				joined++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	ws, err := s.Store.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if joined != 1 || len(ws) != 1 {
		t.Errorf("joined=%d workers=%d, want 1 and 1", joined, len(ws))
	}
}

func TestCreateTokenValidation(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	for name, req := range map[string]workers.TokenRequest{
		"no pool":     {},
		"bad pool":    {Pool: "Release!"},
		"bad label":   {Pool: "trusted", Labels: map[string]string{"": "x"}},
		"ttl too big": {Pool: "trusted", TTL: 365 * 24 * time.Hour},
		"ttl < 0":     {Pool: "trusted", TTL: -time.Hour},
	} {
		if _, err := s.CreateToken(ctx, req); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
