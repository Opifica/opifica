# opifica

Core services of [Opifica](https://github.com/opifica), an open-source CI/CD system built in the EU
where every job runs in a fresh virtual machine.

> **Status:** early development. See the [organisation page](https://github.com/opifica) for the architecture.

## What lives here

| Path | Contents |
|---|---|
| `cmd/controller` | API, provider integration, pipeline engine, scheduler, secrets broker |
| `cmd/worker` | Runs on build machines, manages the hypervisor and the warm pool of VMs |
| `cmd/agent` | Runs inside each VM, executes steps, streams logs and results |
| `cmd/signer` | Optional signing service for release artifacts |
| `cmd/opifica` | CLI for developers and admins |
| `proto/` | API definitions (Protobuf, ConnectRPC) |
| `internal/pipeline` | The pipeline language: YAML parser, CEL expressions, validation, JSON Schema |
| `internal/provider` | Git providers: GitHub, GitLab, Gitea/Forgejo, plain Git |
| `internal/hypervisor` | VM drivers: Firecracker (Linux), Hyper-V (Windows), Virtualization.framework (macOS) |

## Stack

Go · Protobuf + ConnectRPC · PostgreSQL · S3-compatible object storage · CEL · OIDC

## License

[EUPL-1.2](LICENSE)
