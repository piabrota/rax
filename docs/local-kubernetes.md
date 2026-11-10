# Running AX on minikube or MicroK8s

AX's own development cluster is [kind](https://kind.sigs.k8s.io/), set up by
`hack/setup-e2e-kind.sh` in AX. This repository adds the same setup for two
other light distributions, [minikube](https://minikube.sigs.k8s.io/) and
[MicroK8s](https://microk8s.io/), driven by [`just`](https://just.systems/)
recipes. Agent Substrate is installed from its published release images, as
AX's kind setup now does; the task-runner and the control plane are built
from source. By default the task-runner,
the control plane and the CLI are RAX's, from this checkout; `RUNNER=ax`,
`SERVER=ax` and `CLI=ax` swap in AX's Go `ax-task-runner`, `ax-server` and
`ax`, one at a time or together.

> **Status: not yet run end to end.** The scripts were written against AX's
> kind setup and Substrate's installer (v0.6.0's, then v1.0's
> `ate-setup`) and are statically checked
> (shellcheck, `just --dry-run`), but no cluster has been brought up with
> them yet: the environment they were written in cannot give a nested
> cluster network access. Expect to hit rough edges on the first run, and see
> [Limitations](#limitations). What the deployment costs in CPU, memory and
> disk, and what you can try on it, is in
> [minikube-footprint.md](minikube-footprint.md).

## Quick start

You need a Linux host (x86_64 or arm64) with Docker, Go, Cargo, git, curl,
openssl and `just`, plus about 4 CPUs, 8 GiB of memory and 40 GB of disk for the
cluster (see [minikube-footprint.md](minikube-footprint.md) for the minimum and
what the deployment takes). AX is expected next to this checkout (`../ax`); otherwise it is
cloned into `.tmp/local/ax`.

```sh
# minikube
just install minikube     # minikube and kubectl into ~/.local/bin
just up minikube          # a single-node cluster with Substrate's feature gates
just deploy minikube      # registry, Substrate, WorkerPool, Postgres, rax-server

# MicroK8s (Linux only; installs a snap and reconfigures the host, see below)
just install microk8s
just up microk8s
just deploy microk8s

# or all three steps at once
just minikube
just microk8s
```

The first `deploy` installs Substrate from its release images in
`ghcr.io/agent-substrate/substrate`. A Substrate checkout that is not a clean
release tag, or a release without published images, is built from source
instead, which takes a while. Later runs skip Substrate unless its version
changes.

Then use the CLI (`rax` unless `CLI=ax`) against the cluster:

```sh
just ax minikube apply -f ../ax/examples/task.yaml
just ax minikube get tasks
just status minikube
just footprint minikube   # CPU, memory, image sizes and disk in use

# or work directly with kubectl and the CLI binary
source .tmp/local/minikube.env
kubectl -n ax-system logs deploy/ax-server
$AX_BIN get tasks
```

### Iterating on RAX

Change the code, then run `just deploy <distro>` again. It rebuilds the
`rax-server` and task-runner images and the `rax` CLI from this checkout,
pushes the images, and rolls out the new digests.
Substrate, the WorkerPool and the Postgres data are kept.

| Variable | Default | Effect |
|---|---|---|
| `SERVER` | `rax` | `ax` deploys AX's Go `ax-server` instead, for comparison against the same database |
| `RUNNER` | `rax` | `ax` runs tasks on AX's Go `ax-task-runner` image instead |
| `CLI` | `rax` | `ax` makes `just ax` and `just e2e` use AX's Go `ax` instead |
| `RESET_DB` | `false` | `true` wipes AX's Postgres volume first |
| `AX_DIR` | `../ax`, else `.tmp/local/ax` | AX checkout for manifests and the e2e suite (and the Go binaries when one of the above is `ax`) |
| `SUBSTRATE_DIR` | `.tmp/local/substrate` | Use your own Substrate checkout, as is |
| `SUBSTRATE_REF` | the version AX's `go.mod` pins | Substrate version for the managed clone |
| `SUBSTRATE_IMAGE_REPO` | `ghcr.io/agent-substrate/substrate` | Registry holding Substrate's release images, for a mirror |
| `FORCE_INSTALL_SUBSTRATE` | `false` | Reinstall Substrate even if that commit is installed |
| `AX_WORKER_REPLICAS` | `2` | gVisor workers in the `ax-workers` WorkerPool (kind uses 4) |
| `K8S_VERSION` | `v1.37.0` | Kubernetes version for minikube and the installed kubectl |
| `MINIKUBE_PROFILE` | `rax` | minikube profile and kube context name |
| `MINIKUBE_DRIVER` | `docker` | minikube driver |
| `MINIKUBE_CPUS`, `MINIKUBE_MEMORY`, `MINIKUBE_DISK` | `4`, `8g`, `40g` | Node size |
| `MINIKUBE_START_ARGS` | | Extra `minikube start` flags |
| `MICROK8S_CHANNEL` | `1.37/stable` | Snap channel for a new MicroK8s install |

The full list is at the top of [`hack/local/local.sh`](../hack/local/local.sh).

### AX's end-to-end suite

`deploy` builds the E2E variant of the task-runner image (the runner image
plus AX's sample git repository, from AX's `Dockerfile.task-runner.e2e`), so
AX's suite can run against the local cluster, driving the CLI it was given:

```sh
RESET_DB=true just deploy minikube
just e2e minikube
just e2e minikube -test.run TestE2E_ModelCRUD
```

The suite runs from the AX checkout with this cluster's kubeconfig and
context; a kind setup in the same AX checkout is not touched.

### Tearing down

```sh
just down minikube        # deletes the minikube profile
just down microk8s        # removes AX, Substrate and the registry; MicroK8s stays
just uninstall microk8s   # snap remove --purge, and the /var/lib/kubelet link
```

## What `deploy` does

It follows AX's `hack/setup-e2e-kind.sh` step by step and reuses AX's
manifests from `test/e2e/manifests` unchanged:

1. **Registry.** [`hack/local/registry.yaml`](../hack/local/registry.yaml)
   runs `registry:3` on the node's network at port 5001, so the node pulls
   from `localhost:5001` just as kind's nodes do. The host pushes to
   `localhost:5001` as well: MicroK8s runs on the host, and minikube publishes
   the port (`--ports=127.0.0.1:5001:5001`). A `kind-registry` Service in
   `ate-system` stands in for kind's registry container, which Substrate's
   atelet pulls task images from.
2. **Substrate.** Substrate's `ate-setup --kind deploy ate-system` with AX's
   flags, pointed at this cluster's context. For a clean checkout of a release
   tag whose images are published, it installs those images
   (`ATE_IMAGE_REPO`, `ATE_IMAGE_TAG`), as AX's kind setup now does;
   otherwise it builds them with `ko` into the local registry (or runs
   `hack/install-ate-kind.sh` on versions before 1.0). Substrate's kind
   profile fits any single local node: in-cluster Postgres, RustFS for
   snapshots, and no cloud credential provider. The installed commit is
   recorded on the `ate-system` namespace, and a different one is uninstalled
   first.
3. **WorkerPool.** AX's `namespace.yaml` and `workerpool.yaml`, with the gVisor
   worker image of the release pinned by digest, or built by `ko`.
4. **Images.** The task-runner (RAX's `Dockerfile.task-runner` or AX's, then
   AX's E2E variant on top) and the server, pushed and deployed by digest,
   since Substrate only runs digest-pinned images. The CLI (`rax` or AX's
   `ax`) is built for the host into `.tmp/local/bin/ax`, where AX's suite
   looks for it.
5. **Control plane.** AX's `postgres.yaml` and `ax-server.yaml`, the latter
   with the `rax-server` image. RAX accepts the same flags, environment and
   Go-style `--addr=:8080`, so the manifest needs no changes.

| | kind (AX) | minikube | MicroK8s |
|---|---|---|---|
| Cluster | Substrate's `create-kind-cluster.sh` | `minikube start --container-runtime=containerd` with the feature gates | the snap, with flags added to `/var/snap/microk8s/current/args/*` |
| Registry | `kind-registry` container next to the nodes | in-cluster, node port 5001, published on the host | in-cluster, host port 5001 |
| Node tuning | `docker exec` sysctls in the node container | `minikube ssh` sysctls in the node | sysctls on the host |
| Storage | kind's local-path | minikube's `standard` | the `hostpath-storage` addon |
| Database between runs | wiped every run | kept (`RESET_DB=true` to wipe) | kept |

## Limitations

### What still comes from AX (Go)

RAX now ports everything AX ships (see the [parity report](../README.md#parity-with-ax)),
so with the defaults the control plane, every task's runner and the CLI are
Rust. What is still Go:

- **AX's manifests and E2E suite** (`test/e2e`), which is why an AX checkout
  is required. The suite drives whatever CLI `AX_BIN` names, the `rax` binary
  by default.
- **Substrate**, installed from its release images by its Go installer, which
  needs Go on the host (and Docker when it is built from source).

### RAX against AX's suite on a cluster has not been run

AX's E2E scenarios pass against RAX locally: `crates/rax-cli/tests/e2e_local.rs`
replays all seven through the CLI against rax-server, an in-memory Substrate
and real task-runners, and passes with every mix of RAX and AX's Go server,
runner and CLI. It cannot catch what only a cluster does: gVisor sandboxes,
snapshots, the atenet router and the mTLS path of `ssh`, image pulls. Running
`just e2e` here is the first time RAX meets a real Substrate, so expect rough
edges. Some may come from the [known differences](../README.md#known-differences).
Use `SERVER=ax`, `RUNNER=ax` or `CLI=ax` to tell a RAX bug from a setup problem.

### Substrate's requirements

- **Kubernetes with pod certificates.** Substrate's pod certificate controller
  needs the `ClusterTrustBundle`, `ClusterTrustBundleProjection` and
  `PodCertificateRequest` feature gates and the `certificates.k8s.io/v1beta1`
  API, which are off by default. `just up` turns them on; `deploy` refuses a
  cluster without them. `ax-server`'s manifest mounts a `podCertificate` and a
  `clusterTrustBundle` projected volume, so the server cannot start without
  them either. Kubernetes 1.34 or newer is needed; v1.37.0 is the default.
- **A Substrate version AX's kind setup supports.** Substrate v1.0 installs
  with `go run ./cmd/ate-setup --kind`, which AX's `hack/setup-e2e-kind.sh`
  now uses and so does `deploy` (v1.0.0 is the version AX pins at the
  time of writing); older versions with `hack/install-ate-kind.sh` still
  work. Either way Go is needed on the host to run the installer.
- **Docker on the host**, even for MicroK8s: the images are built with it,
  and Substrate's installer checks `docker info` in its kind profile.
- **gVisor only.** Micro-VM sandbox classes (kata with cloud-hypervisor) need
  `/dev/kvm` wired into the node, which these recipes do not do.
- **Kind-profile defaults.** No image credential provider, so tasks can only
  use public images or images in the local registry. Snapshots live in the
  in-cluster RustFS and go away with the cluster.

### Single node, local registry

- The registry answers only on its own node's `localhost:5001`, so
  multi-node clusters (`minikube --nodes`, MicroK8s clustering) are not
  supported. `deploy` warns when it sees more than one node.
- The registry is plain HTTP without authentication, and listens on
  port 5001 of every node interface. On MicroK8s the node is your host, so it
  is reachable from your network unless a firewall blocks it.
- Port 5001 must be free on the host. AX's and Substrate's kind setups use
  the same port for `kind-registry`, so stop that container first.
- Images are built for the node's architecture with `docker build --platform`.
  A node with a different architecture than the host needs Docker's
  emulation (binfmt/QEMU).

### minikube

- The `docker` and `podman` drivers publish the registry on the host. Other
  drivers (kvm2, qemu, hyperkit, VirtualBox) fall back to a
  `kubectl port-forward` during `deploy`, which is slow for Substrate's
  images. `--ports` only applies when the cluster is created, so a profile
  created some other way also gets the fallback.
- Rootless Docker or Podman is not supported: Substrate's gVisor workers
  run as root with `SYS_ADMIN`, `NET_ADMIN` and other capabilities, and
  unconfined AppArmor and seccomp.
- macOS with Docker Desktop should work with the docker driver but has not
  been tried; `fs.inotify` and proxy ARP are then set inside the minikube
  node, not on the Mac.

### MicroK8s

- Linux only, with classic confinement (the snap's default).
- `just up microk8s` changes the host, because MicroK8s's node is the host:
  - adds the feature gates to kube-apiserver, kubelet, kube-controller-manager
    and kube-scheduler, and `--runtime-config` to kube-apiserver, then
    restarts MicroK8s;
  - adds `certs.d/localhost:5001/hosts.toml` so containerd pulls the registry
    over HTTP;
  - links `/var/lib/kubelet` to `/var/snap/microk8s/common/var/lib/kubelet`,
    because atelet registers its device plugin through `/var/lib/kubelet`.
    If `/var/lib/kubelet` already exists for something else, atelet's
    device plugin does not register;
  - sets `net.ipv4.conf.all.proxy_arp=1`, `net.ipv6.conf.all.proxy_ndp=1`
    and `fs.inotify.max_user_instances=8192` on the host (not persisted
    across reboots; re-run `just up microk8s`);
  - enables the `dns` and `hostpath-storage` addons;
  - adds you to the `microk8s` group.
- `just down microk8s` keeps all of that. `just uninstall microk8s` removes
  the snap and the `/var/lib/kubelet` link, but not the sysctls or
  `/var/lib/ax-registry`.
- MicroK8s uses Calico, not kindnet as kind does. Substrate's gVisor
  networking relies on proxy ARP, and has only been exercised on kind.
- `install` keeps an existing MicroK8s on whatever channel it has; Kubernetes
  older than 1.34 cannot run Substrate.
