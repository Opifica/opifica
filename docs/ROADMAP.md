# Roadmap

Where Opifica is and what comes next. Each milestone ends in something that can be run and tested.
Tick items off as they land.

**Defaults chosen for now** (can be revisited):
first Git provider is **Forgejo/Gitea**, **Linux** VMs come before Windows and macOS, and logs are stored on the
**controller's local disk** until S3 storage is added.

## M0 · Foundations ✅

- [x] Organisation, repositories, EUPL-1.2 licence, architecture page
- [x] Pipeline language: parser, validation, CEL conditions, matrix, planning (`internal/pipeline`)
- [x] JSON Schema for editors, tested against the parser (`schema/`)
- [x] API definitions: worker ↔ controller, worker ↔ agent (`proto/`)
- [x] Worker kit: `Hypervisor` interface, warm VM pool, image verification, fake hypervisor (`pkg/worker`)
- [x] CLI: `validate`, `plan`, `schema`
- [x] CI for the core repository

## M1 · First job end to end, without VMs

**Goal:** a run created on the controller is scheduled to a worker, executed by a real agent, and its logs and
result are stored. Tested in ordinary CI, using the fake hypervisor with a real agent process behind it.

### Controller

- [x] **Store:** PostgreSQL migrations and sqlc queries, for workers and join tokens so far
- [ ] **Store:** runs and jobs
- [x] **Certificate authority:** built-in CA that issues worker client certificates and the server certificate
- [x] **Join:** admins create one-time join tokens (`controller token create`); `Join` checks the token and signs
  the worker's CSR; first contact is verified against the CA fingerprint printed with the token
- [x] **Connect stream:** Hello, heartbeats, worker registry, disconnect and reconnect handling, revocation
  (`controller worker list`, `controller worker revoke`)
- [ ] **Leases:** job assignment with a lease renewed by heartbeats; expired leases put the job back in the queue
- [ ] **Scheduler:** match planned jobs to workers by OS, architecture, image, labels and pool; respect `needs`
- [ ] **Runs:** create a run from a pipeline file and an event; track job and run state
- [ ] **Logs:** receive log chunks, drop duplicates after reconnects, store them on disk
- [ ] **Secrets:** encrypted store; requests checked against the job token and policy
- [ ] **Admin config:** static file for pools, routing (event → pool) and secret policy
- [x] **TLS** on the worker API, with worker client certificates accepted
- [x] **mTLS required** for `Connect`; the certificate must be the worker's current one
- [ ] Certificate renewal before the 30-day expiry
- [ ] **Admin API + CLI:** `opifica worker token create`, `opifica runs list`, `opifica logs <job>`

### Worker kit and agent

- [x] Controller client: join (saving the identity to disk), connect, reconnect with backoff, heartbeats
- [ ] Heartbeats filled from the warm pool and job runner
- [ ] Job runner: lease a VM → checkout → run steps through the agent → stream masked logs → collect reports
  and artifacts → destroy the VM
- [ ] Local Git mirror per repository
- [ ] Agent: `Ready`, `Checkout`, `Exec`, `Collect`

### Testing

- [ ] Integration test: controller + PostgreSQL + worker (fake hypervisor) + real agent, in CI
- [ ] Failure tests: worker killed mid-job, lease expiry, reconnect, denied secret

## M2 · Triggered by Git

**Goal:** a push to a Forgejo repository starts a run, and the commit shows its status.

- [ ] Provider interface and Forgejo/Gitea provider: webhooks with signature checks, `FetchFile` at a commit,
  short-lived clone credentials, commit status
- [ ] Routing events to pools from the admin config (pull requests → `untrusted`, `main` → `trusted`,
  tags → `release`)
- [ ] Integration test against a Forgejo container
- [ ] `docker compose` dev environment: PostgreSQL, Forgejo, controller, worker

## M3 · Real Linux VMs

**Goal:** every job runs in a fresh Firecracker microVM.

- [ ] `worker-linux`: Firecracker driver with copy-on-write disks, vsock to the agent, jailer
- [ ] VM networking with egress allowlists (nftables)
- [ ] `images`: Linux golden image (kernel + root filesystem with the agent), built reproducibly
- [ ] E2E test on a machine with `/dev/kvm`
- [ ] Opifica starts building itself (dogfooding)

## M4 · Releases and project automation

Can run alongside M2 and M3.

- [ ] Versioned releases of core (`v0.x` tags) so worker repositories can depend on it safely
- [ ] Release pipeline: cross-compiled binaries, cosign signatures, SBOM, provenance
- [ ] CI for every repository; branch protection on `main`
- [ ] Automated dependency updates

## M5 · Supply-chain security

- [ ] Signing service: release artifacts signed with keys in an HSM or KMS; keys never reach a VM
- [ ] Signed provenance for every job (SLSA)
- [ ] Optional rebuild-and-compare for releases on two different workers
- [ ] Cache rules: untrusted jobs can never write caches that trusted or release jobs read

## M6 · Windows and macOS

Needs self-hosted hardware: an Apple silicon Mac and a Windows machine with Hyper-V.

- [ ] `worker-windows`: Hyper-V driver, differencing disks, warm pool
- [ ] `worker-macos`: Virtualization.framework driver, at most 2 VMs per Mac
- [ ] Image recipes for Windows (Packer) and macOS
- [ ] E2E tests on self-hosted machines

## M7 · Everything else

- [ ] Web UI (Angular): runs, live logs, test results, workers, administration
- [ ] GitHub and GitLab providers; plain Git with polling
- [ ] GitOps sync of the admin config from a Git repository
- [ ] S3-compatible storage for logs and artifacts
- [ ] OIDC login; OpenBao/Vault as a secrets backend
- [ ] OIDC tokens for jobs (short-lived cloud credentials instead of stored keys)
