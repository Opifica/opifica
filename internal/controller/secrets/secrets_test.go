package secrets

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/opifica/opifica/internal/controller/store/db"
)

func TestKeyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	k, err := LoadOrCreateKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, keyFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("key file mode = %v, %v; want 0600", info.Mode().Perm(), err)
	}
	k2, err := LoadOrCreateKey(dir)
	if err != nil || k2.ID() != k.ID() {
		t.Fatalf("reloading gave a different key: %v", err)
	}

	ct, nonce, err := k.seal("DEPLOY_KEY", []byte("hunter2"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ct, []byte("hunter2")) {
		t.Fatal("ciphertext contains the plaintext")
	}
	if v, err := k2.open("DEPLOY_KEY", ct, nonce); err != nil || string(v) != "hunter2" {
		t.Errorf("open = %q, %v", v, err)
	}

	// A ciphertext moved to another secret's row must not decrypt.
	if _, err := k.open("OTHER_SECRET", ct, nonce); err == nil {
		t.Error("ciphertext decrypted under another secret's name")
	}
	tampered := bytes.Clone(ct)
	tampered[0] ^= 1
	if _, err := k.open("DEPLOY_KEY", tampered, nonce); err == nil {
		t.Error("tampered ciphertext decrypted")
	}
	other, _ := LoadOrCreateKey(t.TempDir())
	if _, err := other.open("DEPLOY_KEY", ct, nonce); err == nil {
		t.Error("decrypted with a different key")
	}

	// Every encryption uses a fresh nonce.
	ct2, nonce2, _ := k.seal("DEPLOY_KEY", []byte("hunter2"))
	if bytes.Equal(nonce, nonce2) || bytes.Equal(ct, ct2) {
		t.Error("two encryptions of the same value are identical")
	}
}

func TestNewKeyRejectsBadLength(t *testing.T) {
	if _, err := NewKey(make([]byte, 16)); err == nil {
		t.Error("accepted a 16-byte key")
	}
}

func TestPolicy(t *testing.T) {
	sec := db.Secret{
		Repos: []string{"acme/*"}, Pools: []string{"release"},
		Refs: []string{"refs/heads/main", "refs/tags/v*"},
	}
	run := func(repo, pool, event, ref string) db.Run {
		return db.Run{Repo: repo, Pool: pool, EventType: event, Ref: ref}
	}
	tests := []struct {
		name string
		sec  db.Secret
		run  db.Run
		want string // "" means allowed
	}{
		{"allowed push", sec, run("acme/app", "release", "push", "refs/heads/main"), ""},
		{"allowed tag", sec, run("acme/app", "release", "tag", "refs/tags/v1.2.0"), ""},
		{"other repo", sec, run("evil/app", "release", "push", "refs/heads/main"), "not allowed for repository evil/app"},
		{"nested repo isn't matched by acme/*", sec, run("acme/team/app", "release", "push", "refs/heads/main"), "not allowed for repository acme/team/app"},
		{"other pool", sec, run("acme/app", "untrusted", "push", "refs/heads/main"), "not allowed in pool untrusted"},
		{"other branch", sec, run("acme/app", "release", "push", "refs/heads/feature"), "not allowed for refs/heads/feature"},
		{"pull request", sec, run("acme/app", "release", "pull_request", ""), "not available to pull requests"},
		{"pull request allowed", db.Secret{Repos: []string{"*"}, Pools: []string{"untrusted"}, AllowPullRequests: true},
			run("any/repo", "untrusted", "pull_request", ""), ""},
		{"no refs means any ref", db.Secret{Repos: []string{"acme/**"}, Pools: []string{"trusted"}},
			run("acme/team/app", "trusted", "push", "refs/heads/anything"), ""},
	}
	for _, tt := range tests {
		if got := allowed(tt.sec, tt.run); got != tt.want {
			t.Errorf("%s: allowed() = %q, want %q", tt.name, got, tt.want)
		}
	}
}
