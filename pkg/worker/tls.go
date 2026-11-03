package worker

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// PinnedCAConfig returns a TLS client config for a worker's first contact
// with the controller, before it has the CA certificate. The controller is
// accepted only if it presents a chain ending at a CA whose public key has
// the given fingerprint ("sha256:<hex>", printed by `controller token
// create`), and whose leaf certificate is valid for host.
//
// After joining, the worker uses the CA bundle from the Join response
// instead.
func PinnedCAConfig(fingerprint, host string) (*tls.Config, error) {
	want, ok := strings.CutPrefix(fingerprint, "sha256:")
	if !ok || len(want) != sha256.Size*2 {
		return nil, fmt.Errorf("CA fingerprint must be sha256:<64 hex characters>, got %q", fingerprint)
	}
	want = strings.ToLower(want)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// Standard verification needs the CA, which the worker doesn't have
		// yet. VerifyConnection does the full check against the pinned CA.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyPinned(cs.PeerCertificates, want, host)
		},
	}, nil
}

func verifyPinned(chain []*x509.Certificate, wantHex, host string) error {
	if len(chain) < 2 {
		return errors.New("controller did not send its CA certificate")
	}
	leaf := chain[0]
	for _, c := range chain[1:] {
		sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
		if hex.EncodeToString(sum[:]) != wantHex {
			continue
		}
		if !c.IsCA {
			return errors.New("pinned certificate is not a CA")
		}
		roots := x509.NewCertPool()
		roots.AddCert(c)
		_, err := leaf.Verify(x509.VerifyOptions{
			Roots:     roots,
			DNSName:   host,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		if err != nil {
			return fmt.Errorf("controller certificate: %w", err)
		}
		return nil
	}
	return errors.New("controller's CA does not match the pinned fingerprint")
}
