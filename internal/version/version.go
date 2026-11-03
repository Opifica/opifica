// Package version holds the build version, set at link time:
//
//	go build -ldflags "-X github.com/opifica/opifica/internal/version.Version=v0.1.0"
package version

// Version is the release version, or "dev" for local builds.
var Version = "dev"
