# Roadmap

Where Opifica is and what comes next. Each milestone ends in something that can be run and tested.
Tick items off as they land.

**Decisions so far:** repositories live on **your own git server** (bare repositories managed by Opifica) or on
**GitHub** (runs triggered from a GitHub Actions step); there are no forge integrations. **Linux** VMs come before
Windows and macOS, and logs are stored on the **controller's disk** until S3 storage is added.

## Where we are (2026-11-03)

A pipeline runs end to end. A `git push` to a bare repository with the Opifica hook, or a GitHub Actions step running
`opifica trigger`, starts a run. The controller reads the pipeline at that commit, routes it to a pool, and schedules
its jobs on matching workers. Workers check out the exact commit in a fresh environment and run the steps through the
agent, with the secrets the admin's policy allows. The output is stored and readable with `controller run logs` or
`opifica trigger -wait`. Every job still runs in **devworker**, with **no VM isolation**: real VM drivers are M3.

### Next, in order

1. **Worker certificate renewal.** Worker certificates expire after 30 days and nothing renews them yet, so a
   worker stops connecting a month after joining. Small, and urgent before any real use.
2. **opifica-git** (M2): creating a repository makes the bare repository and installs the hook; developers push
   to it. Needs decisions 1 and 2 below.
3. **worker-linux with Firecracker** (M3): the first real isolation. Needs a Linux machine with `/dev/kvm`; the
   development machine has it.
4. **Releases** (M4): `v0.x` tags so the worker repositories can depend on core, plus signed binaries so the
   GitHub workflow doesn't need `go install`.
5. The rest of M1: an admin API, so the `opifica` CLI and the web UI work remotely; storing test reports and
   artifacts (those steps are skipped with a note for now).

### Decisions needed

1. **How developers push to opifica-git:** HTTPS with access tokens, SSH with keys (an SSH server in Go, no
   system sshd), or both. Recommended: HTTPS first, then SSH.
2. **Who may create repositories and push:** proposed: users and tokens managed by the admin through the CLI,
   with read or write access per repository; login through your identity provider (OIDC) later, with the web UI.
3. **How GitHub runners reach the controller:** expose the controller's port, or use a self-hosted runner
   inside your network.
4. **Hardware for Windows and macOS workers** (M6): an Apple silicon Mac and a Windows machine with Hyper-V.

### Known limitations

- Log deduplication is kept in memory: after a controller restart, a resent log chunk can appear twice.
- The controller issues itself a new server certificate at every start (workers trust the CA, so this is fine).
- Windows: cancelling a step kills the step's process but not its children yet (needs job objects).
- Repository credentials: tokens only; SSH keys aren't supported yet.

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
- [x] **Store:** runs and jobs
- [x] **Certificate authority:** built-in CA that issues worker client certificates and the server certificate
- [x] **Join:** admins create one-time join tokens (`controller token create`); `Join` checks the token and signs
  the worker's CSR; first contact is verified against the CA fingerprint printed with the token
- [x] **Connect stream:** Hello, heartbeats, worker registry, disconnect and reconnect handling, revocation
  (`controller worker list`, `controller worker revoke`)
- [x] **Leases:** job assignment with a lease renewed by heartbeats; expired leases put the job back in the queue
  (up to 3 attempts); jobs a disconnected worker never started go back at once
- [x] **Scheduler:** match planned jobs to workers by OS, architecture, image, labels and pool; respect `needs`;
  safe with several controllers (`FOR UPDATE SKIP LOCKED`)
- [x] **Runs:** create a run from a pipeline file and an event; track job and run state; cancel
- [x] **Logs:** receive log chunks, drop duplicates after reconnects, store them on disk (`controller run logs`)
- [x] **Secrets:** encrypted store (AES-256-GCM); requests checked against the worker holding the job, the job
  token, the job's `secrets:` list and the admin's policy (repositories, pools, refs, pull requests); every
  request audited (`controller secret set|list|delete|access`)
- [ ] **Admin config:** static file for pools, routing (event → pool) and secret policy
- [x] **TLS** on the worker API, with worker client certificates accepted
- [x] **mTLS required** for `Connect`; the certificate must be the worker's current one
- [ ] Certificate renewal before the 30-day expiry
- [x] **Admin CLI (direct database access):** `controller token create`, `worker list|revoke`, `run create|list|show|cancel`
- [ ] **Admin API** so the `opifica` CLI and web UI can do the same remotely; `opifica logs <job>`

### Worker kit and agent

- [x] Controller client: join (saving the identity to disk), connect, reconnect with backoff, heartbeats
- [x] Heartbeats filled from the warm pool and job runner
- [x] Job runner: lease a VM → checkout → run steps through the agent → stream masked logs → destroy the VM;
  updates survive reconnects through an outbox
- [ ] Collect and store test reports and artifacts (steps are skipped with a note for now)
- [x] Local Git mirror per repository
- [x] Agent: `Ready`, `Checkout`, `Exec`, `Collect`; secrets masked in output; process trees killed on cancel and timeout
- [x] `devworker`: try everything on one machine without VMs (no isolation)

### Testing

- [x] Integration test: controller + PostgreSQL + worker (fake hypervisor) + real agent, in CI
- [x] Failure tests: worker killed mid-job, lease expiry, reconnect, denied secret

## M2 · Triggered by git: your own server or GitHub

**Goal:** a `git push` starts a run, whether the repository is hosted by Opifica or on GitHub.

- [x] Repository registry: clone URL, routing rules (event + ref → pool, first match wins), trigger tokens
  (`controller repo create|list|delete|token create`)
- [x] Trigger API; the controller reads the pipeline file at the commit itself and picks the pool from the
  repository's rules
- [x] `opifica hook post-receive`: the git hook for bare repositories; prints the run for the pusher
- [x] `opifica trigger -wait` for GitHub Actions: passes or fails with the run, prints its output, writes a
  step summary ([example workflow](../examples/github-actions.yml))
- [x] Optional read credential per repository, encrypted, given to workers with each job
- [ ] **opifica-git**: create a repository through the API or CLI, which creates the bare repository and installs
  the hook; serves git to developers (HTTPS and/or SSH: to decide); users and access tokens
- [ ] Workers fetch hosted repositories with short-lived, read-only tokens
- [ ] `docker compose` dev environment: PostgreSQL, controller, opifica-git, devworker

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
- [ ] Secrets key in a KMS or HSM instead of a file; key rotation
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
- [ ] GitOps sync of the admin config from a Git repository
- [ ] S3-compatible storage for logs and artifacts
- [ ] OIDC login; OpenBao/Vault as a secrets backend
- [ ] OIDC tokens for jobs (short-lived cloud credentials instead of stored keys)
