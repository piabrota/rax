# rax

<p align="center">
<img alt="代理 (dairi, an agent: one who acts in another's place) brushed in its sixteen strokes, among stars, inside an orbit" src="docs/drawings/cosmos.svg" width="520"/>
<br/>
<sub>代理 · <i>dairi</i> · one who acts in another's place · <a href="docs/drawings/README.md">the drawing</a></sub>
</p>

Rustic Agentic Runtime: a pure-Rust port of [AX](https://github.com/google/ax),
the declarative orchestrator for running agentic tasks on
[Agent Substrate](https://github.com/agent-substrate/substrate).

RAX keeps AX's `ax.io/v1alpha1` API: the same gRPC service and the same
Task, Workspace and Model manifests, so a manifest written for AX works here.

## Status

The port lands in four steps, each its own pull request:

| Step | Scope | Status |
|---|---|---|
| 1 | Cargo workspace, API types generated from `ax.proto`, YAML manifests, validation | done |
| 2 | `rax-server`: gRPC API, in-memory and PostgreSQL stores, distributed locks | done |
| 3 | Substrate client, controller and reconciliation | done |
| 4 | `rax` CLI, `rax-task-runner`, tunnel, end-to-end parity tests | done |

## Layout

| Crate | Go counterpart | Role |
|---|---|---|
| `crates/rax-api` | `pkg/apis/v1alpha1` | Generated types and gRPC service, YAML encoding, validation |
| `crates/rax-store` | `internal/store`, `internal/lock` | In-memory and PostgreSQL stores, in-memory and PostgreSQL locks |
| `crates/rax-substrate` | `internal/substrate` | The Substrate control API client (gRPC over rustls), actor templates, readiness probes, and an in-memory Substrate for tests |
| `crates/rax-server` | `internal/server`, `internal/controller`, `cmd/ax-server` | The gRPC service and `/healthz` on one port, the task reconciler, and the `rax-server` binary |
| `crates/rax-guest` | Substrate's `env/guest`, `internal/guest` | The guest data plane inside each task (process and filesystem services, which `ssh` uses) and its client |
| `crates/rax-task-runner` | `runner`, `internal/metadata`, `internal/workspace`, `cmd/ax-task-runner` | The process inside every task container: workspace setup, the probe, metadata and guest server, and the supervised task command |
| `crates/rax-claude-code-runner` | `cmd/runners/claude-code-runner` | A task runner that turns `spec.config` into a headless `claude --print` session; the runner of the Claude Code image (`Dockerfile.claude-code`) |
| `crates/rax-cli` | `cmd/ax`, `internal/tunnel` | The `rax` CLI: `apply`, `get`, `describe`, `suspend`, `resume`, `delete`, `ssh`, `ctx`, `tunnel`, `version`, with kubectl tunnels and router mTLS |

The PostgreSQL store uses AX's schema and the same protobuf JSON documents,
so RAX and AX can read each other's records.

The proto is compiled in pure Rust with [protox](https://crates.io/crates/protox),
so building needs no `protoc`. [pbjson](https://crates.io/crates/pbjson-build)
generates serde impls that follow the protobuf JSON mapping, which is what
protojson gives the Go implementation, and YAML is bridged through it.

## Running the server

```sh
# Everything in memory, for a quick local run:
cargo run -p rax-server -- --store memory:// --addr 127.0.0.1:8080

# Against PostgreSQL (the default store URL is postgres://ax@localhost:5432/ax):
AX_STORE=postgres://rax@localhost:5432/rax cargo run -p rax-server
```

`curl localhost:8080/healthz` answers `ok`.

## Using the CLI

`rax` takes the same commands and flags as AX's `ax`, prints the same output,
and shares its tunnels in `~/.ax/tunnels`:

```sh
cargo install --path crates/rax-cli
rax --server localhost:8080 apply -f task.yaml
rax get tasks
rax ssh my-task -- ls -la /workspace
```

Without `--server` or `$AX_SERVER`, it port-forwards to `svc/ax-server` in the
current Kubernetes context, as `ax` does.

## The task-runner image

`Dockerfile.task-runner` builds a drop-in replacement for AX's
`ax-task-runner` image: the same base image and tools, with the runner at
`/usr/local/bin/ax-task-runner`, where actor templates expect it. Point
`rax-server --default-task-image` (or `AX_DEFAULT_TASK_IMAGE`) at it, or set
`spec.image` per task.

## Running on a local cluster

`just` recipes bring up AX on a single-node minikube or MicroK8s cluster,
with Agent Substrate, AX's task-runner and `rax-server` all built from
source and deployed with AX's own manifests:

```sh
just minikube     # install minikube, create the cluster, deploy
just microk8s     # the same on MicroK8s (Linux)
just ax minikube get tasks
```

See [docs/local-kubernetes.md](docs/local-kubernetes.md) for the steps,
the settings, and the limitations: the server, task-runner and CLI are RAX's
by default (`SERVER=ax`, `RUNNER=ax` and `CLI=ax` swap in AX's), AX's E2E
suite is still Go, Substrate needs pod-certificate feature gates, and only
single-node clusters with gVisor sandboxes are supported. The setup has not
been run end to end on a cluster yet. For the cost (about 1.5 CPUs and 2 GiB
requested, 6 GB on the node and 10 GB of builds on the host), the minimum
node size and what you can try on minikube, see
[docs/minikube-footprint.md](docs/minikube-footprint.md); `just footprint
minikube` measures a running cluster.

The server reconciles tasks on Agent Substrate. Inside the cluster the
defaults find it (`api.ate-system.svc.cluster.local:443`, with the mounted
service account token and trust bundle). Elsewhere, point it at Substrate
with `--substrate-endpoint`, and `--substrate-ca-file`,
`--substrate-token-file`, `--substrate-insecure-tls` or
`--substrate-plaintext` as needed. Like AX, each task gets an actor named
after it and a per-task actor template named `<task>-tmpl-<digest>`.
Setting `ATENET_ROUTER_ADDR` also probes workspace readiness through the
router.

A published `rax-server` image (linux/amd64) is at
`ghcr.io/piabrota/rax-server`: every push to `main` that changes the server's
sources publishes `main` and `sha-<commit>` from `Dockerfile.server`
(`.github/workflows/image.yml`, whose run summary prints the digest to pin).
The image also has the `rax` CLI, for Jobs and CronJobs that apply and resume
tasks in the cluster: `command: [rax, --server, ax-server.ax-system:8080, ...]`.

## Running without Substrate: the pod runner

`rax-server --runner=pod` runs tasks as plain Kubernetes pods instead of
actors on Agent Substrate, for clusters that cannot host Substrate (a small
single node, no gVisor). It is RAX's own mode, not AX's: there is no sandbox,
snapshot, workspace setup or `ssh`. Each resume runs the task once, in a new
pod in one namespace:

| Task | Pod |
|---|---|
| created, or suspended | no pod running; a running one is deleted |
| resumed | a new pod; the task's older pods are deleted first |
| `Running`, `Resuming` | the pod is `Running`, `Pending` |
| `Suspended` | the pod exited 0, or there is none |
| `Failed` | the pod exited non-zero or cannot start (its image, a root user); the Ready condition says why |
| deleted | its pods are deleted |

A finished pod stays until the next resume, so `kubectl logs` reads its
output. The pod gets the task's `spec.env`, `AX_TASK_NAME`,
`AX_TASK_ATESPACE` and `AX_TASK_YAML`; its limits, with requests equal to
them, from `spec.resources.limits` or the server's defaults; `restartPolicy:
Never`; no privilege escalation, no capabilities, the runtime's seccomp
profile, and `runAsNonRoot`, so task images set a non-root `USER`.

| Flag | Default | |
|---|---|---|
| `--runner` (`RAX_RUNNER`) | `substrate` | `pod` for this mode |
| `--pod-namespace` | `ax-tasks` | where task pods run |
| `--pod-env-from-secret` | none | a Secret in that namespace whose keys become every task's environment; repeat for more. Tasks cannot name Secrets themselves |
| `--pod-service-account` | none | the pods' service account; without one, no token is mounted |
| `--pod-cpu-limit`, `--pod-memory-limit` | `500m`, `256Mi` | limits for tasks that set none |

The server talks to the API as its own pod's service account, which needs
`get`, `list`, `create` and `delete` on `pods` in that namespace. A
`ResourceQuota` there bounds how many tasks run at once: a create the quota
refuses leaves the task `Pending`, to retry.

## Parity with AX

**RAX covers everything AX ships**, as of AX's `main` at
[`f0228f1`](https://github.com/google/ax/commit/f0228f1) (2026-10-30): the
control plane (`ax-server`), the process inside each task container
(`ax-task-runner`, including the guest services `ax ssh` talks to), the
Claude Code runner and its image, and the CLI (`ax`, with its tunnels). The
one AX package not ported, `internal/model`, is a Gemini client library that
no AX binary uses. Counting AX's hand-written, non-test Go, 7,593 of 8,215
lines (92%) have a Rust counterpart; the rest is `internal/model`. RAX also ports
the part of Substrate's `env` module that AX's runner embeds (`guest`, about
1,300 lines), so no Go runs inside a RAX task.

The API, the manifests, the PostgreSQL records, the actor templates, the
task-runner's ports and endpoints, the guest gRPC services and the CLI's
output and tunnel files are the same, so the pieces mix: AX's `ax` works
against `rax-server` and RAX's runner, `rax` works against AX's `ax-server`
and runner, and so on (see [What is tested](#what-is-tested)).

### Feature by feature

| AX package | What it does | RAX | Status |
|---|---|---|---|
| `pkg/apis/v1alpha1` (types, resources) | Task, Workspace and Model types, YAML and JSON encoding, name and spec validation, CPU and memory quantities | `rax-api` | done |
| `internal/store`, `store/memory`, `store/postgres` | Resource store; PostgreSQL with AX's schema | `rax-store` | done (no TLS to PostgreSQL yet) |
| `internal/lock`, `store/postgres/lock.go` | Per-resource locks; PostgreSQL leases with renewal and takeover | `rax-store` | done |
| `internal/server` | The `ax.v1alpha1.AX` gRPC API (all 14 RPCs), actor-owned status, `/healthz` | `rax-server` | done |
| `cmd/ax-server` | Server binary, flags and environment | `rax-server` binary | done (same flags) |
| `internal/substrate` | Substrate control API client: TLS, token, retries, atespaces, templates, actors | `rax-substrate` | done |
| `internal/controller` | Task reconciler: per-task templates, explicit `Suspend`, `Resume` and `Delete`, workspace readiness, delete cleanup | `rax-server::controller` | done |
| `cmd/ax` | CLI: `apply`, `get`, `describe`, `suspend`, `resume`, `delete`, `ssh`, `ctx`/`context`, `tunnel list`/`stop`, `version` | `rax-cli` (`rax`) | done (same flags, output and exit codes) |
| `internal/tunnel` | CLI tunnels: per-context state, `kubectl port-forward`, router TLS through a PodCertificateRequest | `rax-cli::tunnel`, `rax-cli::router_tls` | done (same state and cache files) |
| `internal/guest` | Client for the in-actor guest daemon (`ax ssh`: exec, stdin, signals, Ctrl-C twice to leave) | `rax-guest::client` | done |
| Substrate `env/guest` (embedded by AX's runner) | Guest daemon: process service with spooled output, stdin, signals, limits; filesystem service; reflection | `rax-guest` | done |
| `cmd/ax-task-runner`, `runner` | The process inside each task container: runs setup, then supervises the task's command | `rax-task-runner` | done (same flags, `-flag` spellings too) |
| `internal/metadata` | `/healthz`, `/readyz` and guest gRPC on one port; task and workspace YAML on loopback | `rax-task-runner::metadata` | done |
| `internal/workspace` | Workspace setup on first run: git clones with retry, files, skills, marker files | `rax-task-runner::workspace` | done |
| `cmd/runners/claude-code-runner`, `Dockerfile.claude-code` | Task runner that builds a headless `claude --print` invocation from `spec.config` and the Workspace's MCP servers; the image with Claude Code installed | `rax-claude-code-runner`, `Dockerfile.claude-code`, `examples/claude-code` | done (same arguments, byte for byte in the ported tests) |
| `internal/model` | Gemini client built from a Model resource (API key from secret or store) | none | not ported: no AX binary uses it |
| `Dockerfile.*`, `deploy/`, `hack/`, `test/e2e` | Images, Kubernetes manifests, kind-based end-to-end tests | `Dockerfile.server`, `Dockerfile.task-runner`, `hack/local`, `tests/e2e_local.rs` | done, reusing AX's manifests and E2E suite: RAX images and a minikube/MicroK8s setup instead of kind, and a local replay of the suite |

### Known differences

- PostgreSQL connections have no TLS: `sslmode=require` fails, `disable` works.
- YAML is written with 2-space indentation where Go writes 4: in `get`
  output, the runner's metadata endpoints and `AX_TASK_YAML`. The per-task
  template digest therefore differs from AX's for the same task, so after
  switching from AX to RAX, each task gets a new template the first time it
  is reconciled.
- The in-memory store lists newest first; AX's sorts by key.
- Conditions show `context: message` rather than gRPC's
  `rpc error: code = ... desc = ...` text. The CLI's own errors do use gRPC's
  text, so scripts matching `NotFound` and the like keep working.
- Deleting an actor stops waiting after 2 minutes instead of relying only on
  the request deadline.
- Without a CA file, the Substrate client trusts the Mozilla roots
  (webpki-roots) rather than the system roots.
- The CLI is called `rax`, prints `rax version v1alpha1`, and names itself
  `rax` in usage and hints. `apply` treats an unset spec and an empty one as
  equal when deciding between `unchanged` and `configured`.
- `apply` splits multi-document files on `---` lines rather than with a YAML
  stream parser; ordinary manifests, empty documents and comments behave as
  in AX.
- The task-runner gives `AX_METADATA_URL` and `spec.env` to the task command
  and to `ssh` processes directly rather than through its own environment,
  logs through `tracing` instead of `slog`, and waits at most 5 seconds for
  open connections on shutdown.

### What is tested

167 tests, all run in CI (`cargo test --all`, with a PostgreSQL 16 service).
They port the Go tests for the packages above and add some of their own.

| Suite | Tests | Covers |
|---|---|---|
| `rax-api` unit | 21 | YAML round trips and strict decoding (unknown fields rejected), lenient decoding, Workspace MCP and Model secret-key spellings, task config, workspace binding, name/meta/task/file validation, resource validation, CPU and memory parsing |
| `rax-store` unit | 12 | One conformance suite run on both the memory and the PostgreSQL store (CRUD, defaults, paging, not-found), concurrent schema creation, records with unknown fields, lock serialization, cancellation, lease renewal and takeover of expired leases, a crashed server's tasks kept and its locks freed after a restart, URL redaction |
| `rax-server` unit | 15 | Actor state to task phase and Ready, stored status keeps only AX's fields, `/healthz`, template naming and digest, the template name pattern, worker and router addresses, Go-style `:port` listen addresses; the pod runner's manifests, pod phases, retryable errors, one run per resume and label-scoped listing |
| `rax-server` `tests/service.rs` | 17 | gRPC round trip over the real router, invalid names, workspace binding, suspend and resume lifecycle, delete (terminating, cleanup failure, status write failure), suspend success and failure, actor state read from Substrate, new tasks start suspended, failed and transiently failed provisioning, a later success clearing a stored failure, a suspend cancelling a failed delete, creation time kept, concurrent creates serialized |
| `rax-server` `tests/controller.rs` | 17 | Ports of AX's reconciler tests against an in-memory Substrate: actor named after the task, worker IP, default image, suspend and failed suspend, rejected template (bare Substrate message, no fallback), transient template failure, workspace readiness polled directly and through the router and kept across suspend and resume, no new template on status changes, resource limits and their failure paths, delete removes only the task's templates, waits for the actor, times out, transient failures and recovery; plus a full create, resume, get, suspend and delete through the service |
| `rax-substrate` unit | 12 | Retry and backoff rules, transient errors, template building, snapshot location, resource limits, TLS config, HTTP readiness probe |
| `rax-substrate` `tests/client.rs` | 13 | Client against an in-memory Substrate: atespace, template and actor creation, revert or recreate of crashed actors, waiting out deleting actors, resume retries (`ResourceExhausted` not retried), delete polling, missing actors and timeout, template deletion retries, pagination, token re-read on every call |
| `rax-substrate` `tests/tls.rs` | 1 | A real TLS connection: CA file, authority override, wrong server name rejected, unreadable CA, skip-verify |
| `rax-guest` unit | 9 | Output spooling and replay from offsets, exit codes and signal exits (128+n), stdin writes and close, signals to the process group, the concurrency cap, log truncation, the default timeout, pruning, start failures, timeouts, path cleaning and confinement, the signal table |
| `rax-task-runner` unit | 8 | First-run marker, git clone into a subdirectory with depth, git errors withholding the marker, inlined files, marker names, clone destinations, spec loading with unknown fields and empty specs |
| `rax-task-runner` `tests/runner.rs` | 8 | Ports of AX's runner, metadata and guest client tests: command env, working directory and exit code, serving after the command exits, stopping the command's process group, waiting without a command, bound workspace setup and readiness, metadata only on loopback, guest services only with `spec.debug`, exec with stdin, empty stdin, exit codes and forwarded signals |
| `rax-claude-code-runner` unit | 7 | Ports of AX's claude-code-runner tests: rejected configs, the minimal invocation, flags by value type in sorted order, the Workspace's MCP servers as `--mcp-config` byte for byte, Go-style JSON escaping and numbers, `spec.command` left alone |
| `rax-cli` unit | 15 | Ports of AX's argument parsing, `ssh` argument and Ctrl-C escalation tests, tunnel context names and the port-forward pattern, AX's tunnel state files, tabwriter-style tables, Go `%v` values, document splitting, kinds, worker addresses, router TLS configs |
| `rax-cli` `tests/cli.rs` | 5 | Ports of AX's CLI tests, running the `rax` binary against rax-server: `apply` atespaces for every kind, immutable tasks, `unchanged` and `configured`, empty and invalid documents, `get` aliases and missing resources, exact `describe` output, `delete`, and the offline commands (`version`, `ctx`, `tunnel list`/`stop`, usage) |
| `rax-cli` `tests/e2e_local.rs` | 7 | AX's seven E2E scenarios, replayed without Kubernetes through the CLI binary against rax-server, an in-memory Substrate and real task-runners: CLI version, workspace and model CRUD, task lifecycle and Substrate sync, metadata server and guest exec, workspace setup with files and git, suspend and resume persistence, describe and atespace isolation |

The local E2E replay can swap in AX's Go binaries (`RAX_E2E_CLI`,
`RAX_E2E_RUNNER`, `RAX_E2E_SERVER` with `RAX_E2E_STORE`; `just e2e-local`).
All seven scenarios pass with: RAX everywhere; AX's `ax` against RAX; AX's
`ax-task-runner` under RAX; AX's `ax-server` (PostgreSQL) with RAX's CLI and
runner; `rax-server` on PostgreSQL with AX's CLI and runner; and AX's Go
stack alone, which shows the replay asks what AX's suite asks.

Not tested yet: against a real Agent Substrate (only the in-memory one), the
`ssh` path through the atenet router with mTLS, the task-runner image
inside a gVisor sandbox, an AX and a RAX server sharing one database, and
AX's end-to-end suite on a cluster. `just e2e <distro>` runs that suite
against RAX on a local cluster ([docs/local-kubernetes.md](docs/local-kubernetes.md)),
but it has not been run yet: the environment RAX is developed in cannot run
nested Kubernetes.

## Development

```sh
cargo test                                   # or: just test
cargo clippy --all-targets -- -D warnings    # or: just lint
cargo fmt --all --check
```

The PostgreSQL tests run when `RAX_TEST_POSTGRES_URL` points at a database
they may wipe, for example `postgres://rax@localhost:5432/rax_test`.

The README drawing is pure Nix (`drawings/`, see [docs/drawings](docs/drawings/README.md)):
`just drawing` writes it again, and `nix flake check` checks it.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Parts derived from AX keep their
Apache 2.0 license ([LICENSE-APACHE](LICENSE-APACHE)). The drawing and its pen
(`drawings/pen.nix`, from piabrota/nix-os, ported there from
[pirsig](https://github.com/h0ffmann/pirsig)'s Rust pen) are under the same
MPL-2.0 as the rest of RAX.
