# opifica

Core of [Opifica](https://github.com/opifica), an open-source CI/CD system built in the EU
where every job runs in a fresh virtual machine.

> **Status:** early development. A pipeline runs end to end: the controller schedules its jobs, a worker checks
> out the exact commit and runs each job's commands through the agent with the secrets the admin allows, and
> the output is stored. Real VMs and Git providers come next; see [docs/ROADMAP.md](docs/ROADMAP.md).
> See the [organisation page](https://github.com/opifica) for the architecture.

## What lives here

| Path | Contents | Status |
|---|---|---|
| `internal/pipeline` | The pipeline language: YAML parser, validation, CEL conditions, matrix expansion, planning | Working |
| `schema/` | JSON Schema for pipeline files, for editor autocompletion | Working |
| `cmd/opifica` | CLI: `validate`, `plan`, `schema` | Working |
| `proto/` | API definitions: worker ↔ controller, worker ↔ guest agent (Protobuf, ConnectRPC) | Draft |
| `pkg/worker` | Shared worker kit: `Hypervisor` interface, warm VM pool, controller client, Git mirrors, job runner, `worker.Run` | Working |
| `pkg/worker/workertest` | Fake hypervisor for testing worker code without VMs | Working |
| `cmd/controller` | Join tokens, worker joining over TLS, the Connect stream, worker revocation | Working |
| `internal/controller/scheduler` | Runs and jobs, matching jobs to workers, leases, retries, cancellation | Working |
| `internal/controller/logs` | Job output on the controller's disk | Working |
| `internal/controller/secrets` | Encrypted secrets, handed only to jobs the admin's policy allows; every request audited | Working |
| | Git providers, admin API | Not started |
| `cmd/agent`, `internal/agent` | Runs inside each VM: checkout, running steps, masking secrets in output, killing process trees on cancel and timeout | Working |
| `cmd/devworker` | A worker without VMs for trying Opifica on one machine (no isolation) | Working |
| `cmd/signer` | Signing service for release artifacts | Not started |

The workers live in their own repositories, so each one contains only its own OS-specific code:
[worker-linux](https://github.com/opifica/worker-linux) (Firecracker),
[worker-windows](https://github.com/opifica/worker-windows) (Hyper-V) and
[worker-macos](https://github.com/opifica/worker-macos) (Virtualization.framework).
Each implements `worker.Hypervisor` and gets everything else from `pkg/worker`.

## The pipeline language

Pipelines live in `.opifica/pipeline.yaml` in each repository.

```yaml
on:
  push:         { branches: [main, "release/*"] }
  pull_request: { branches: [main] }
  tag:          { pattern: "v*" }

jobs:
  test:
    matrix: { os: [linux, windows, macos] }
    runs-on:
      os: ${{ matrix.os }}
    steps:
      - run: make test
      - report: junit build/test-results/*.xml

  release:
    needs: test
    if: event.tag != ""
    runs-on: { os: macos, arch: arm64, image: xcode-16 }
    secrets: [NOTARY_TOKEN]
    steps:
      - run: make dist
        secrets: [NOTARY_TOKEN]
      - upload: dist/*.dmg
      - sign: dist/*.dmg
```

- **Triggers:** `push` and `pull_request` filter on branch globs (for pull requests, the target branch).
  `tag` filters on tag globs. `*` stays within one segment; `feature/**` matches everything below `feature/`.
- **`runs-on`:** `linux`, `windows` or `macos`, or a mapping with `os`, `arch` (`amd64`/`arm64`), `image` and `labels`.
- **Steps:** each step has exactly one of `run`, `report`, `upload` or `sign`.
- **`if`:** a [CEL](https://cel.dev) expression over `event` and `matrix`.
  Event fields: `type`, `branch`, `base_branch`, `tag`, `sha`, `ref`, `pull_request`.
- **`${{ matrix.<key> }}` and `${{ event.<field> }}`** are substituted in strings when a run is planned.
  YAML can't parse them inside inline `{ }` or `[ ]`, so put them on their own line or quote them.
- **Secrets:** a job lists the secrets it may use, and each step names the ones it gets. The admin policy decides
  whether the job actually receives them; the pipeline file can't grant itself access.

The full example in [`internal/pipeline/testdata/valid/full.yaml`](internal/pipeline/testdata/valid/full.yaml)
shows every feature.

### Editor support

Add this line at the top of your pipeline file for autocompletion and validation in editors that use the
YAML language server (VS Code, IntelliJ, Neovim):

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/Opifica/opifica/main/schema/pipeline.schema.json
```

### Try it

```console
$ go run ./cmd/opifica validate internal/pipeline/testdata/valid/full.yaml
internal/pipeline/testdata/valid/full.yaml: OK (4 jobs)

$ go run ./cmd/opifica plan -event tag -tag v1.2.0 internal/pipeline/testdata/valid/full.yaml
Tag v1.2.0: 9 job(s) to run, 0 skipped

JOB                    OS       ARCH   IMAGE     STEPS  NEEDS
Test (linux, amd64)    linux    amd64  default   2      -
...
release                macos    arm64  xcode-16  4      test, lint, windows-installer
```

Errors come with their line and column, all at once:

```console
$ opifica validate broken.yaml
broken.yaml:4:14: jobs.build.runs-on.os: unknown os "solaris" (one of: linux, windows, macos)
broken.yaml:9:12: jobs.deploy.needs: unknown job "biuld"
broken.yaml:11:5: unknown field "secret"
3 problem(s) found
```

## Development

Requires Go 1.27 or later.

```sh
make tools      # install buf, the protoc plugins and sqlc (once)
make            # lint, test and build into bin/
make generate   # after changing proto/ or SQL queries; generated code is committed
```

Database tests need PostgreSQL. Either set `OPIFICA_TEST_DATABASE_URL` to a server the tests may create databases
on, or install the PostgreSQL server binaries (e.g. `apt install postgresql-17`): the tests then start their own
throwaway server. Without either, those tests are skipped.

### Running a pipeline on one machine

`devworker` runs jobs without VMs, directly on your machine, with **no isolation**. Use it only with code you trust.

```sh
export OPIFICA_DATABASE_URL=postgres://localhost/opifica
go build -o bin/ ./cmd/...

bin/controller token create -pool trusted        # prints a join token and the CA fingerprint
bin/controller serve -addr 127.0.0.1:8443 &       # TLS; keep it running

bin/devworker join -controller https://127.0.0.1:8443 -token opj_... -ca-fingerprint sha256:...
bin/devworker run  -controller https://127.0.0.1:8443 &

# Until Git providers exist, start runs by hand:
bin/controller run create -repo acme/app -clone-url "$PWD" -pool trusted \
    -sha "$(git rev-parse HEAD)" -file .opifica/pipeline.yaml
bin/controller run show <run-id>
bin/controller run logs <run-id>
```

### Secrets

The admin decides who gets a secret; a pipeline can only ask for it. The value is read from standard input:

```sh
printf %s "$TOKEN" | bin/controller secret set DEPLOY_TOKEN -repo 'acme/*' -pool trusted -ref refs/heads/main
bin/controller secret list                  # policies, never values
bin/controller secret access DEPLOY_TOKEN   # who asked, and whether they got it
```

A job gets `DEPLOY_TOKEN` only if it lists it under `secrets:`, the step that needs it names it too, and the run
matches the policy: repository, pool and ref. Pull requests get no secrets unless the secret is set with
`-allow-pull-requests`. Values are masked as `***` in job output.

## Stack

Go · Protobuf + ConnectRPC · PostgreSQL · S3-compatible object storage · CEL · OIDC

## License

[EUPL-1.2](LICENSE)
