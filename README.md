# opifica

Core services of [Opifica](https://github.com/opifica), an open-source CI/CD system built in the EU
where every job runs in a fresh virtual machine.

> **Status:** early development. See the [organisation page](https://github.com/opifica) for the architecture.

## What lives here

| Path | Contents |
|---|---|
| `cmd/controller` | API, provider integration, pipeline engine, scheduler, secrets broker |
| `cmd/agent` | Runs inside each VM, executes steps, streams logs and results |
| `cmd/signer` | Optional signing service for release artifacts |
| `cmd/opifica` | CLI for developers and admins |
| `proto/` | API definitions (Protobuf, ConnectRPC) |
| `internal/pipeline` | The pipeline language: YAML parser, CEL expressions, validation, JSON Schema |
| `internal/provider` | Git providers: GitHub, GitLab, Gitea/Forgejo, plain Git |
| `pkg/worker` | Shared worker kit: controller connection, leases, warm-pool manager, Git mirror, image verification and the `Hypervisor` interface |

The workers themselves live in their own repositories, so each one contains only its own OS-specific code:
[worker-linux](https://github.com/opifica/worker-linux) (Firecracker),
[worker-windows](https://github.com/opifica/worker-windows) (Hyper-V) and
[worker-macos](https://github.com/opifica/worker-macos) (Virtualization.framework).

## Stack

Go · Protobuf + ConnectRPC · PostgreSQL · S3-compatible object storage · CEL · OIDC

## License

[EUPL-1.2](LICENSE)
