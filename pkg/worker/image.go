package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Image is a golden VM image on the worker's disk.
type Image struct {
	// Name is what pipelines refer to in runs-on.image, e.g. "default".
	Name string
	// Digest is "sha256:<hex>". The worker refuses to boot an image whose
	// contents don't match it.
	Digest string
	// Path is the image file (or the directory's main disk) on the host.
	Path string
}

// ErrDigestMismatch means an image on disk differs from its expected digest:
// it is corrupt or has been tampered with.
var ErrDigestMismatch = errors.New("image digest mismatch")

// VerifyImage checks that the file at img.Path matches img.Digest. Golden
// images can be tens of gigabytes, so workers verify them when they are
// installed or changed, not before every boot.
func VerifyImage(img Image) error {
	want, ok := strings.CutPrefix(img.Digest, "sha256:")
	if !ok || len(want) != sha256.Size*2 {
		return fmt.Errorf("image %q: digest must be sha256:<64 hex characters>, got %q", img.Name, img.Digest)
	}
	got, err := FileDigest(img.Path)
	if err != nil {
		return fmt.Errorf("image %q: %w", img.Name, err)
	}
	if got != "sha256:"+strings.ToLower(want) {
		return fmt.Errorf("image %q: %w: want %s, got %s", img.Name, ErrDigestMismatch, img.Digest, got)
	}
	return nil
}

// FileDigest returns the "sha256:<hex>" digest of a file.
func FileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
