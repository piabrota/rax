#!/usr/bin/env bash
# Runs AX on a local single-node minikube or MicroK8s cluster: Agent Substrate
# from its release images (or built from source), AX's own manifests, and the control plane, task-runner and
# CLI built from this checkout (rax) or from AX. It follows AX's kind setup
# (hack/setup-e2e-kind.sh in AX) step by step, swapping the kind-only parts.
# See docs/local-kubernetes.md, which also lists what does not work.
#
# Usage: hack/local/local.sh <command> <minikube|microk8s> [args...]
#
# Commands:
#   install    Install the distribution and kubectl
#   up         Create (minikube) or configure (MicroK8s) the cluster
#   deploy     Deploy the registry, Substrate, Postgres and the AX control plane
#   status     Show the AX and Substrate workloads
#   footprint  Report the CPU, memory, disk and image sizes the deployment uses
#   ax         Run the CLI (rax or ax) against the cluster, e.g. `ax minikube get tasks`
#   e2e        Run AX's end-to-end suite against the cluster
#   down       Delete the minikube cluster, or remove AX and Substrate from MicroK8s
#   uninstall  Remove MicroK8s itself (minikube: same as down)
#
# Configuration (environment):
#   SERVER               Control plane to deploy: rax (default, this checkout) or ax (AX's Go ax-server)
#   RUNNER               Task-runner image: rax (default, this checkout) or ax (AX's Go ax-task-runner)
#   CLI                  CLI that `ax` and `e2e` use: rax (default, this checkout) or ax (AX's Go ax)
#   AX_DIR               AX checkout (default: ../ax if present, else a clone in .tmp/local/ax)
#   AX_REPO, AX_REF      What to clone when AX_DIR is unset (default: https://github.com/google/ax.git, main)
#   SUBSTRATE_DIR        Substrate checkout, used as is (default: a clone in .tmp/local/substrate)
#   SUBSTRATE_REPO       Substrate repository (default: https://github.com/agent-substrate/substrate.git)
#   SUBSTRATE_REF        Substrate version for the managed clone (default: the one AX's go.mod pins)
#   SUBSTRATE_IMAGE_REPO Registry holding Substrate release images (default: ghcr.io/agent-substrate/substrate)
#   FORCE_INSTALL_SUBSTRATE  true to reinstall Substrate even if that commit is installed
#   AX_WORKER_REPLICAS   gVisor workers in the ax-workers WorkerPool (default: 2)
#   RESET_DB             true to wipe AX's Postgres volume before deploying
#   K8S_VERSION          Kubernetes version for minikube and kubectl (default: v1.37.0)
#   MINIKUBE_PROFILE     minikube profile and kube context name (default: rax)
#   MINIKUBE_DRIVER      minikube driver (default: docker)
#   MINIKUBE_CPUS, MINIKUBE_MEMORY, MINIKUBE_DISK  Node size (default: 4, 8g, 40g)
#   MINIKUBE_START_ARGS  Extra `minikube start` flags
#   MINIKUBE_VERSION     minikube release to install (default: latest)
#   MICROK8S_CHANNEL     MicroK8s snap channel (default: 1.37/stable)
#   INSTALL_DIR          Where `install` puts minikube and kubectl (default: ~/.local/bin)

set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="${REPO_ROOT}/.tmp/local"

COMMAND="${1:-}"
DISTRO="${2:-}"
shift 2 || true

SERVER="${SERVER:-rax}"
RUNNER="${RUNNER:-rax}"
CLI="${CLI:-rax}"
K8S_VERSION="${K8S_VERSION:-v1.37.0}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-rax}"
MINIKUBE_DRIVER="${MINIKUBE_DRIVER:-docker}"
MICROK8S_CHANNEL="${MICROK8S_CHANNEL:-1.37/stable}"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"
SUBSTRATE_REPO="${SUBSTRATE_REPO:-https://github.com/agent-substrate/substrate.git}"
SUBSTRATE_IMAGE_REPO="${SUBSTRATE_IMAGE_REPO:-ghcr.io/agent-substrate/substrate}"
AX_REPO="${AX_REPO:-https://github.com/google/ax.git}"
AX_REF="${AX_REF:-main}"
# AX's ax-server manifest and Substrate's bootstrap owners both assume this.
export AX_SYSTEM_NAMESPACE=ax-system
export AX_WORKER_REPLICAS="${AX_WORKER_REPLICAS:-2}"
# The registry in registry.yaml, as the node and the host both see it.
REGISTRY_ADDR=localhost:5001

# Feature gates Substrate's pod certificate controller needs; kind gets the
# same ones from Substrate's hack/create-kind-cluster.sh.
FEATURE_GATES="ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true"
RUNTIME_CONFIG="certificates.k8s.io/v1beta1=true"

log() { printf '\n==> %s\n' "$*" >&2; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

case "${DISTRO}" in
  minikube)
    KUBE_CONTEXT="${MINIKUBE_PROFILE}"
    ;;
  microk8s)
    KUBE_CONTEXT=microk8s
    ;;
  *) usage ;;
esac
export KUBECONFIG="${TMP_DIR}/kubeconfig-${DISTRO}"
export KUBE_CONTEXT
ENV_FILE="${TMP_DIR}/${DISTRO}.env"
AX_BIN="${TMP_DIR}/bin/ax"
mkdir -p "${TMP_DIR}/bin"
export PATH="${INSTALL_DIR}:${PATH}"

k() {
  kubectl --kubeconfig "${KUBECONFIG}" --context "${KUBE_CONTEXT}" "$@"
}

need() {
  local tool
  for tool in "$@"; do
    command -v "${tool}" > /dev/null 2>&1 || die "${tool} is required but not on PATH"
  done
}

host_os() { uname -s | tr '[:upper:]' '[:lower:]'; }
host_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) die "unsupported architecture $(uname -m)" ;;
  esac
}

# mk runs microk8s, through sudo unless the user is in the microk8s group.
mk() {
  if id -nG | tr ' ' '\n' | grep -qx microk8s; then
    microk8s "$@"
  else
    sudo microk8s "$@"
  fi
}

## --------------------------------------
## install
## --------------------------------------

install_kubectl() {
  if command -v kubectl > /dev/null 2>&1; then
    log "kubectl already installed ($(command -v kubectl))."
    return
  fi
  log "Installing kubectl ${K8S_VERSION} to ${INSTALL_DIR}..."
  mkdir -p "${INSTALL_DIR}"
  curl -fsSLo "${INSTALL_DIR}/kubectl" \
    "https://dl.k8s.io/release/${K8S_VERSION}/bin/$(host_os)/$(host_arch)/kubectl"
  chmod +x "${INSTALL_DIR}/kubectl"
}

check_build_tools() {
  local tool missing=()
  for tool in docker go git curl openssl; do
    command -v "${tool}" > /dev/null 2>&1 || missing+=("${tool}")
  done
  if ((${#missing[@]})); then
    warn "deploy also needs: ${missing[*]} (Docker builds the images; Go builds Substrate and AX)"
  fi
}

install_minikube() {
  if command -v minikube > /dev/null 2>&1; then
    log "minikube already installed: $(minikube version --short 2> /dev/null || true)"
  else
    local version="${MINIKUBE_VERSION:-latest}"
    log "Installing minikube (${version}) to ${INSTALL_DIR}..."
    mkdir -p "${INSTALL_DIR}"
    curl -fsSLo "${INSTALL_DIR}/minikube" \
      "https://storage.googleapis.com/minikube/releases/${version}/minikube-$(host_os)-$(host_arch)"
    chmod +x "${INSTALL_DIR}/minikube"
  fi
  install_kubectl
  check_build_tools
  case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *) warn "${INSTALL_DIR} is not on your PATH; the just recipes add it themselves" ;;
  esac
}

install_microk8s() {
  [[ "$(host_os)" == linux ]] || die "MicroK8s runs natively on Linux only; use minikube on $(host_os)"
  need snap sudo
  if snap list microk8s > /dev/null 2>&1; then
    log "MicroK8s already installed ($(snap list microk8s | awk 'NR == 2 { print $4 }')); not changing its channel."
  else
    log "Installing MicroK8s from snap channel ${MICROK8S_CHANNEL} (needs sudo)..."
    sudo snap install microk8s --classic --channel="${MICROK8S_CHANNEL}"
  fi
  if ! id -nG | tr ' ' '\n' | grep -qx microk8s; then
    log "Adding ${USER} to the microk8s group (log out and back in to drop sudo)..."
    sudo usermod -a -G microk8s "${USER}"
  fi
  install_kubectl
  check_build_tools
}

## --------------------------------------
## up
## --------------------------------------

up_minikube() {
  need minikube kubectl
  log "Starting minikube profile '${MINIKUBE_PROFILE}' (Kubernetes ${K8S_VERSION}, driver ${MINIKUBE_DRIVER})..."
  local args=(
    start -p "${MINIKUBE_PROFILE}"
    --driver="${MINIKUBE_DRIVER}"
    --container-runtime=containerd
    --kubernetes-version="${K8S_VERSION}"
    --cpus="${MINIKUBE_CPUS:-4}"
    --memory="${MINIKUBE_MEMORY:-8g}"
    --disk-size="${MINIKUBE_DISK:-40g}"
    --feature-gates="${FEATURE_GATES}"
    --extra-config=apiserver.runtime-config="${RUNTIME_CONFIG}"
  )
  # Publish the in-cluster registry on the host; other drivers fall back to a
  # port-forward during deploy.
  case "${MINIKUBE_DRIVER}" in
    docker | podman) args+=(--ports="127.0.0.1:5001:5001") ;;
  esac
  # shellcheck disable=SC2086 # MINIKUBE_START_ARGS is a list of flags
  minikube "${args[@]}" ${MINIKUBE_START_ARGS:-}

  log "Preparing the node for gVisor sandboxes..."
  # Like Substrate's kind setup: proxy ARP/NDP for gVisor's loopback pod
  # networking, and enough inotify instances for many sandboxes.
  minikube ssh -p "${MINIKUBE_PROFILE}" -- \
    "sudo sysctl -q -w net.ipv4.conf.all.proxy_arp=1 fs.inotify.max_user_instances=8192 &&
     sudo sysctl -q -e -w net.ipv6.conf.all.proxy_ndp=1 &&
     sudo mkdir -p /etc/containerd/certs.d/${REGISTRY_ADDR} &&
     printf '[host.\"http://${REGISTRY_ADDR}\"]\n  capabilities = [\"pull\", \"resolve\"]\n' |
       sudo tee /etc/containerd/certs.d/${REGISTRY_ADDR}/hosts.toml > /dev/null"
  k get nodes
}

# ensure_flag makes the MicroK8s args file contain flag=value, merging value
# into an existing comma-separated flag. Prints "changed" when it edits.
ensure_flag() {
  local file="$1" flag="$2" value="$3" current item merged
  current="$(sudo sed -n "s/^${flag}=//p" "${file}" | tail -n1 | tr -d '"')"
  if [[ -z "${current}" ]]; then
    echo "${flag}=${value}" | sudo tee -a "${file}" > /dev/null
    echo changed
    return
  fi
  merged="${current}"
  IFS=, read -ra items <<< "${value}"
  for item in "${items[@]}"; do
    [[ ",${merged}," == *",${item%%=*}="* ]] && continue
    merged="${merged},${item}"
  done
  if [[ "${merged}" != "${current}" ]]; then
    sudo sed -i "s|^${flag}=.*|${flag}=${merged}|" "${file}"
    echo changed
  fi
}

up_microk8s() {
  need microk8s kubectl sudo
  log "Waiting for MicroK8s..."
  mk status --wait-ready > /dev/null
  log "Enabling the dns and hostpath-storage addons..."
  mk enable dns
  mk enable hostpath-storage

  log "Enabling Substrate's feature gates (${FEATURE_GATES})..."
  local args_dir=/var/snap/microk8s/current/args changed="" component
  for component in kube-apiserver kubelet kube-controller-manager kube-scheduler; do
    changed+="$(ensure_flag "${args_dir}/${component}" --feature-gates "${FEATURE_GATES}")"
  done
  changed+="$(ensure_flag "${args_dir}/kube-apiserver" --runtime-config "${RUNTIME_CONFIG}")"

  # Pull localhost:5001 over plain HTTP, as MicroK8s does for its own registry.
  local certs_dir="${args_dir}/certs.d/${REGISTRY_ADDR}"
  if ! sudo test -f "${certs_dir}/hosts.toml"; then
    sudo mkdir -p "${certs_dir}"
    printf '[host."http://%s"]\n  capabilities = ["pull", "resolve"]\n' "${REGISTRY_ADDR}" |
      sudo tee "${certs_dir}/hosts.toml" > /dev/null
    changed+=changed
  fi

  if [[ -n "${changed}" ]]; then
    log "Restarting MicroK8s to apply the new flags..."
    mk stop
    mk start
    mk status --wait-ready > /dev/null
  fi

  # atelet mounts /var/lib/kubelet/{plugins,device-plugins} from the host, but
  # MicroK8s keeps kubelet's state under the snap.
  local kubelet_dir=/var/snap/microk8s/common/var/lib/kubelet
  if [[ ! -e /var/lib/kubelet ]]; then
    log "Linking /var/lib/kubelet to ${kubelet_dir} for atelet's device plugin..."
    sudo ln -s "${kubelet_dir}" /var/lib/kubelet
  elif [[ "$(readlink -f /var/lib/kubelet)" != "${kubelet_dir}" ]]; then
    warn "/var/lib/kubelet exists and is not MicroK8s's kubelet directory; atelet's device plugin will not register"
  fi

  log "Setting proxy ARP/NDP and inotify limits on this host (MicroK8s's node is the host)..."
  sudo sysctl -q -w net.ipv4.conf.all.proxy_arp=1 fs.inotify.max_user_instances=8192
  sudo sysctl -q -e -w net.ipv6.conf.all.proxy_ndp=1

  mk config > "${KUBECONFIG}"
  chmod 600 "${KUBECONFIG}"
  k get nodes
}

## --------------------------------------
## deploy
## --------------------------------------

# resolve_ax_dir sets AX_DIR to an AX checkout, cloning one if needed.
resolve_ax_dir() {
  if [[ -z "${AX_DIR:-}" && -d "${REPO_ROOT}/../ax/.git" ]]; then
    AX_DIR="${REPO_ROOT}/../ax"
  fi
  if [[ -z "${AX_DIR:-}" ]]; then
    AX_DIR="${TMP_DIR}/ax"
    if [[ ! -d "${AX_DIR}/.git" ]]; then
      log "Cloning AX (${AX_REF}) from ${AX_REPO}..."
      git clone --quiet --branch "${AX_REF}" "${AX_REPO}" "${AX_DIR}"
    fi
  fi
  AX_DIR="$(cd "${AX_DIR}" && pwd)"
  [[ -f "${AX_DIR}/test/e2e/manifests/ax-server.yaml" ]] || die "AX_DIR=${AX_DIR} is not an AX checkout"
  log "Using AX at ${AX_DIR} ($(git -C "${AX_DIR}" rev-parse --short HEAD))"
}

# resolve_substrate_dir sets SUBSTRATE_DIR, cloning Substrate at the version
# AX's go.mod pins unless a checkout is given.
resolve_substrate_dir() {
  if [[ -n "${SUBSTRATE_DIR:-}" ]]; then
    SUBSTRATE_DIR="$(cd "${SUBSTRATE_DIR}" && pwd)"
  else
    local ref="${SUBSTRATE_REF:-}"
    if [[ -z "${ref}" ]]; then
      ref="$(awk '$1 == "go.ate.dev/substrate" { print $2; exit }' "${AX_DIR}/go.mod")"
      [[ -n "${ref}" ]] || die "cannot find the Substrate version in ${AX_DIR}/go.mod; set SUBSTRATE_REF"
    fi
    # A Go pseudo-version names a commit.
    if [[ "${ref}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-.*-([0-9a-f]{12})$ ]]; then
      ref="${BASH_REMATCH[1]}"
    fi
    SUBSTRATE_DIR="${TMP_DIR}/substrate"
    if [[ ! -d "${SUBSTRATE_DIR}/.git" ]]; then
      log "Cloning Substrate from ${SUBSTRATE_REPO}..."
      git clone --quiet "${SUBSTRATE_REPO}" "${SUBSTRATE_DIR}"
    fi
    if ! git -C "${SUBSTRATE_DIR}" checkout --quiet --detach "${ref}" 2> /dev/null; then
      git -C "${SUBSTRATE_DIR}" fetch --quiet --tags origin
      git -C "${SUBSTRATE_DIR}" checkout --quiet --detach "${ref}"
    fi
  fi
  [[ -x "${SUBSTRATE_DIR}/hack/install-ate-kind.sh" || -d "${SUBSTRATE_DIR}/cmd/ate-setup" ]] ||
    die "${SUBSTRATE_DIR} has neither hack/install-ate-kind.sh nor cmd/ate-setup; use a Substrate version AX supports"
  SUBSTRATE_COMMIT="$(git -C "${SUBSTRATE_DIR}" rev-parse HEAD)"
  log "Using Substrate at ${SUBSTRATE_DIR} (${SUBSTRATE_COMMIT:0:12})"
  resolve_substrate_release
}

# The ko:// worker image in AX's WorkerPool, and the name Substrate publishes
# it under (the last element of the import path).
worker_ko_ref() {
  awk '$1 == "workerImage:" { print $2; exit }' "${AX_DIR}/test/e2e/manifests/workerpool.yaml"
}

# Every Substrate image the kind install and the WorkerPool use.
substrate_images() {
  local worker
  worker="$(worker_ko_ref)"
  printf '%s\n' ateapi atecontroller atelet atenet "${worker##*/}" podcertcontroller objectstore-plugin envoy-dataplane
}

# image_digest prints the digest the registry holds for repo:tag, where repo
# is host/path, or fails if it has none. A registry that answers 401 gets an
# anonymous pull token, which is enough for public images such as
# Substrate's releases on ghcr.io. As in AX's kind setup.
image_digest() {
  local tag="$2" host="${1%%/*}" path="${1#*/}" url headers challenge realm service token
  url="https://${host}/v2/${path}/manifests/${tag}"
  local accept=(
    -H 'Accept: application/vnd.oci.image.index.v1+json'
    -H 'Accept: application/vnd.oci.image.manifest.v1+json'
    -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json'
    -H 'Accept: application/vnd.docker.distribution.manifest.v2+json'
  )
  headers="$(curl -sSI "${accept[@]}" "${url}" 2> /dev/null | tr -d '\r')" || return 1
  # The last status line, past any proxy's CONNECT response.
  if [[ "$(awk '/^HTTP\// { code = $2 } END { print code }' <<< "${headers}")" == "401" ]]; then
    challenge="$(awk 'tolower($1) == "www-authenticate:"' <<< "${headers}")"
    realm="$(sed -nE 's/.*realm="([^"]+)".*/\1/p' <<< "${challenge}")"
    service="$(sed -nE 's/.*service="([^"]+)".*/\1/p' <<< "${challenge}")"
    [[ -n "${realm}" ]] || return 1
    token="$(curl -fsS -G "${realm}" --data-urlencode "service=${service}" \
      --data-urlencode "scope=repository:${path}:pull" 2> /dev/null |
      sed -nE 's/.*"(access_)?token" *: *"([^"]+)".*/\2/p')"
    [[ -n "${token}" ]] || return 1
    headers="$(curl -fsSI "${accept[@]}" -H "Authorization: Bearer ${token}" "${url}" 2> /dev/null | tr -d '\r')" || return 1
  fi
  awk 'tolower($1) == "docker-content-digest:" { print $2; found = 1 } END { exit !found }' <<< "${headers}"
}

# resolve_substrate_release sets SUBSTRATE_RELEASE to the release tag whose
# published images to install, as AX's kind setup now does, or leaves it
# empty to build Substrate from source with ko. Release images need a clean
# checkout at a release tag (the manifests come from the checkout, the images
# from the release), an ate-setup with --image-repo, and every image published
# under SUBSTRATE_IMAGE_REPO. AX's setup refuses anything else; this one
# builds instead, so a Substrate checkout with local changes still deploys.
resolve_substrate_release() {
  SUBSTRATE_RELEASE=""
  local tag img
  tag="$(git -C "${SUBSTRATE_DIR}" tag --points-at HEAD | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$' | head -n1 || true)"
  if [[ -z "${tag}" ]]; then
    log "Substrate ${SUBSTRATE_COMMIT:0:12} is not a release tag; building it from source."
    return
  fi
  if [[ -n "$(git -C "${SUBSTRATE_DIR}" status --porcelain --untracked-files=no)" ]]; then
    log "Substrate checkout has local changes; building it from source instead of using the ${tag} release images."
    return
  fi
  if ! grep -q -- "--image-repo" "${SUBSTRATE_DIR}/cmd/ate-setup/commands.md" 2> /dev/null; then
    log "Substrate ${tag} cannot install pre-built images (ate-setup has no --image-repo); building it from source."
    return
  fi
  while read -r img; do
    if ! image_digest "${SUBSTRATE_IMAGE_REPO}/${img}" "${tag}" > /dev/null; then
      log "${SUBSTRATE_IMAGE_REPO}/${img}:${tag} not found; building Substrate from source."
      return
    fi
  done < <(substrate_images)
  SUBSTRATE_RELEASE="${tag}"
  log "Using Substrate ${tag} release images from ${SUBSTRATE_IMAGE_REPO}."
}

# render substitutes only the AX_* variables in one of AX's E2E manifests.
MANIFEST_VARS=(AX_SYSTEM_NAMESPACE AX_WORKER_REPLICAS AX_SERVER_IMAGE AX_DEFAULT_TASK_IMAGE)
render() {
  local manifest="${AX_DIR}/test/e2e/manifests/$1" sed_args=() var
  for var in "${MANIFEST_VARS[@]}"; do
    sed_args+=(-e "s|\${${var}}|${!var:-}|g")
  done
  sed "${sed_args[@]}" "${manifest}"
}

check_cluster() {
  k get --raw /readyz > /dev/null 2>&1 ||
    die "cannot reach the ${DISTRO} cluster (context ${KUBE_CONTEXT}); run \`just up ${DISTRO}\` first"
  k get --raw /apis/certificates.k8s.io/v1beta1 2> /dev/null | grep -q clustertrustbundles ||
    die "the cluster does not serve ClusterTrustBundles; recreate it with \`just up ${DISTRO}\` (needs ${FEATURE_GATES})"
}

PORT_FORWARD_PID=""
stop_port_forward() {
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" 2> /dev/null || true
  fi
}

deploy_registry() {
  log "Deploying the local registry (${REGISTRY_ADDR} on the node)..."
  k apply -f "${SCRIPT_DIR}/registry.yaml"
  k -n ax-registry rollout status deployment/registry --timeout=180s
  if ! curl -fsS -o /dev/null "http://${REGISTRY_ADDR}/v2/" 2> /dev/null; then
    log "Registry not published on the host; port-forwarding ${REGISTRY_ADDR} while deploying..."
    k -n ax-registry port-forward svc/registry 5001:5000 > /dev/null 2>&1 &
    PORT_FORWARD_PID=$!
    trap stop_port_forward EXIT
  fi
  local _
  for _ in $(seq 1 30); do
    curl -fsS -o /dev/null "http://${REGISTRY_ADDR}/v2/" 2> /dev/null && return 0
    sleep 1
  done
  die "the registry is not reachable at http://${REGISTRY_ADDR}/v2/ from this host"
}

# uninstall_substrate removes Substrate and its cluster-scoped resources, as
# AX's kind setup does before installing another Substrate version.
uninstall_substrate() {
  k -n "${AX_SYSTEM_NAMESPACE}" delete workerpool ax-workers --ignore-not-found --timeout=30s > /dev/null 2>&1 ||
    k -n "${AX_SYSTEM_NAMESPACE}" patch workerpool ax-workers --type=merge -p '{"metadata":{"finalizers":null}}' > /dev/null 2>&1 || true
  k delete validatingadmissionpolicybinding,validatingadmissionpolicy -l app.kubernetes.io/part-of=ate --ignore-not-found > /dev/null 2>&1 || true
  k delete validatingadmissionpolicybinding,validatingadmissionpolicy sandboxconfig-assets --ignore-not-found > /dev/null 2>&1 || true
  if ! k delete sandboxconfig --all --ignore-not-found --timeout=30s > /dev/null 2>&1; then
    k get sandboxconfig -o name 2> /dev/null | while read -r config; do
      k patch "${config}" --type=merge -p '{"metadata":{"finalizers":null}}' > /dev/null 2>&1 || true
    done
  fi
  k get crd -o name 2> /dev/null | grep '\.ate\.dev$' | while read -r crd; do
    k delete "${crd}" --ignore-not-found > /dev/null 2>&1 || true
  done
  k delete namespace ate-system --ignore-not-found --wait=true > /dev/null 2>&1 || true
  k label nodes --all ate.dev/substrate-version- > /dev/null 2>&1 || true
}

deploy_substrate() {
  local installed
  installed="$(k get namespace ate-system -o jsonpath='{.metadata.annotations.substrate-commit}' 2> /dev/null || true)"
  if [[ "${FORCE_INSTALL_SUBSTRATE:-false}" != "true" && "${installed}" == "${SUBSTRATE_COMMIT}" ]] &&
    k -n ate-system get deployment ate-api-server > /dev/null 2>&1; then
    log "Substrate ${SUBSTRATE_COMMIT:0:12} already installed; waiting for ate-api-server..."
    k -n ate-system wait --for=condition=Available deployment/ate-api-server --timeout=180s
    return
  fi
  if [[ -n "${installed}" ]]; then
    log "Removing Substrate ${installed:0:12}..."
    uninstall_substrate
    k apply -f "${SCRIPT_DIR}/registry.yaml" > /dev/null
  fi

  if [[ -n "${SUBSTRATE_RELEASE}" ]]; then
    log "Installing Substrate ${SUBSTRATE_RELEASE} from its release images..."
    (
      cd "${SUBSTRATE_DIR}"
      ATE_IMAGE_REPO="${SUBSTRATE_IMAGE_REPO}" ATE_IMAGE_TAG="${SUBSTRATE_RELEASE}" \
        go run ./cmd/ate-setup --kind --context="${KUBE_CONTEXT}" \
        deploy ate-system \
        --rollout-timeout="${SUBSTRATE_ROLLOUT_TIMEOUT:-5m}" \
        --credential-provider='{"enabled":false}' \
        --ateapi-authz-bootstrap-owners="system:serviceaccount:ate-system:ate-client,system:serviceaccount:${AX_SYSTEM_NAMESPACE}:ax-server" \
        --ingress-allowed-spiffe-ids="spiffe://cluster.local/ns/${AX_SYSTEM_NAMESPACE}/sa/ax-server"
    )
    k annotate namespace ate-system "substrate-commit=${SUBSTRATE_COMMIT}" --overwrite > /dev/null
    return
  fi

  log "Building and installing Substrate ${SUBSTRATE_COMMIT:0:12} (this takes a while the first time)..."
  # Substrate's kind profile fits a single local node: in-cluster Postgres,
  # RustFS for snapshots, no cloud credential provider. Its only kind-specific
  # assumptions are the kind-registry name, which registry.yaml provides, and
  # a reachable Docker daemon, which builds the images anyway.
  # Substrate 1.0 dropped hack/install-ate-kind.sh for `ate-setup --kind`,
  # which AX's kind setup now runs; older versions still have the script.
  local install_cmd=(./hack/install-ate-kind.sh)
  if [[ ! -f "${SUBSTRATE_DIR}/hack/install-ate-kind.sh" ]]; then
    install_cmd=(go run ./cmd/ate-setup --kind --context="${KUBE_CONTEXT}")
  fi
  (
    cd "${SUBSTRATE_DIR}"
    KUBECTL_CONTEXT="${KUBE_CONTEXT}" KO_DOCKER_REPO="${REGISTRY_ADDR}" \
      KO_DEFAULTPLATFORMS="linux/${NODE_ARCH}" "${install_cmd[@]}" \
      deploy ate-system \
      --rollout-timeout="${SUBSTRATE_ROLLOUT_TIMEOUT:-5m}" \
      --credential-provider='{"enabled":false}' \
      --ateapi-authz-bootstrap-owners="system:serviceaccount:ate-system:ate-client,system:serviceaccount:${AX_SYSTEM_NAMESPACE}:ax-server" \
      --ingress-allowed-spiffe-ids="spiffe://cluster.local/ns/${AX_SYSTEM_NAMESPACE}/sa/ax-server"
  )
  k annotate namespace ate-system "substrate-commit=${SUBSTRATE_COMMIT}" --overwrite > /dev/null
}

deploy_workers() {
  log "Deploying namespace ${AX_SYSTEM_NAMESPACE} and the ax-workers WorkerPool (${AX_WORKER_REPLICAS} gVisor replicas)..."
  render namespace.yaml | k apply -f -
  if [[ -n "${SUBSTRATE_RELEASE}" ]]; then
    # The release's worker image, pinned by digest as Substrate requires.
    local ko_ref worker_ref
    ko_ref="$(worker_ko_ref)"
    worker_ref="${SUBSTRATE_IMAGE_REPO}/${ko_ref##*/}:${SUBSTRATE_RELEASE}@$(image_digest "${SUBSTRATE_IMAGE_REPO}/${ko_ref##*/}" "${SUBSTRATE_RELEASE}")"
    render workerpool.yaml | sed "s|${ko_ref}|${worker_ref}|" | k apply -f -
  else
    (
      cd "${SUBSTRATE_DIR}"
      render workerpool.yaml |
        KO_DOCKER_REPO="${REGISTRY_ADDR}" ./hack/run-tool.sh ko resolve --platform="linux/${NODE_ARCH}" -f -
    ) | k apply -f -
  fi
  local _
  for _ in $(seq 1 60); do
    k -n "${AX_SYSTEM_NAMESPACE}" get deployment ax-workers > /dev/null 2>&1 && break
    sleep 2
  done
  k -n "${AX_SYSTEM_NAMESPACE}" rollout status deployment/ax-workers --timeout=300s
}

# push builds an image, pushes it to the local registry, and prints its
# digest reference (Substrate only runs images pinned by digest).
push() {
  local tag="$1" context="$2"
  shift 2
  docker build -q --platform "linux/${NODE_ARCH}" -t "${tag}" "$@" "${context}" > /dev/null
  docker push -q "${tag}" > /dev/null
  docker inspect --format='{{index .RepoDigests 0}}' "${tag}"
}

build_images() {
  log "Building the CLI (${CLI}, host) and what runs in the cluster (linux/${NODE_ARCH})..."
  (
    cd "${AX_DIR}"
    mkdir -p "bin/linux_${NODE_ARCH}"
    if [[ "${CLI}" == "ax" ]]; then
      CGO_ENABLED=0 go build -o "${AX_BIN}" ./cmd/ax
    fi
    if [[ "${RUNNER}" == "ax" ]]; then
      CGO_ENABLED=0 GOOS=linux GOARCH="${NODE_ARCH}" go build -o "bin/linux_${NODE_ARCH}/ax-task-runner" ./cmd/ax-task-runner
    fi
    if [[ "${SERVER}" == "ax" ]]; then
      CGO_ENABLED=0 GOOS=linux GOARCH="${NODE_ARCH}" go build -o "bin/linux_${NODE_ARCH}/ax-server" ./cmd/ax-server
    fi
  )
  if [[ "${CLI}" == "rax" ]]; then
    (cd "${REPO_ROOT}" && cargo build --release --locked -p rax-cli)
    cp "${REPO_ROOT}/target/release/rax" "${AX_BIN}"
  fi

  log "Building and pushing images to ${REGISTRY_ADDR}..."
  # The E2E task-runner image is the runner image plus a sample git repo, so
  # AX's end-to-end suite can run against this cluster too.
  case "${RUNNER}" in
    rax)
      docker build -q --platform "linux/${NODE_ARCH}" -t "${REGISTRY_ADDR}/ax-task-runner:base" \
        -f "${REPO_ROOT}/Dockerfile.task-runner" "${REPO_ROOT}" > /dev/null
      ;;
    ax)
      docker build -q --platform "linux/${NODE_ARCH}" -t "${REGISTRY_ADDR}/ax-task-runner:base" \
        --build-arg TARGETARCH="${NODE_ARCH}" -f "${AX_DIR}/Dockerfile.task-runner" "${AX_DIR}" > /dev/null
      ;;
  esac
  AX_DEFAULT_TASK_IMAGE="$(push "${REGISTRY_ADDR}/ax-task-runner:local" "${AX_DIR}" \
    --build-arg BASE_IMAGE="${REGISTRY_ADDR}/ax-task-runner:base" -f "${AX_DIR}/test/e2e/Dockerfile.task-runner.e2e")"
  case "${SERVER}" in
    rax)
      AX_SERVER_IMAGE="$(push "${REGISTRY_ADDR}/rax-server:local" "${REPO_ROOT}" -f "${REPO_ROOT}/Dockerfile.server")"
      ;;
    ax)
      AX_SERVER_IMAGE="$(push "${REGISTRY_ADDR}/ax-server:local" "${AX_DIR}" \
        --build-arg TARGETARCH="${NODE_ARCH}" -f "${AX_DIR}/Dockerfile.server")"
      ;;
  esac
  export AX_SERVER_IMAGE AX_DEFAULT_TASK_IMAGE
  printf '  AX_SERVER_IMAGE=%s\n  AX_DEFAULT_TASK_IMAGE=%s\n' "${AX_SERVER_IMAGE}" "${AX_DEFAULT_TASK_IMAGE}" >&2
}

deploy_control_plane() {
  log "Deploying ax-postgres and the control plane (${SERVER}) in ${AX_SYSTEM_NAMESPACE}..."
  if [[ "${RESET_DB:-false}" == "true" ]]; then
    k -n "${AX_SYSTEM_NAMESPACE}" delete statefulset ax-postgres --ignore-not-found --wait
    k -n "${AX_SYSTEM_NAMESPACE}" delete pvc -l app.kubernetes.io/name=ax-postgres --ignore-not-found --wait
  fi
  # Postgres reads the password only when it initializes an empty volume, so
  # keep the Secret across deploys.
  k -n "${AX_SYSTEM_NAMESPACE}" get secret ax-postgres > /dev/null 2>&1 ||
    k -n "${AX_SYSTEM_NAMESPACE}" create secret generic ax-postgres --from-literal=password="$(openssl rand -hex 24)"
  render postgres.yaml | k apply -f -
  k -n "${AX_SYSTEM_NAMESPACE}" rollout status statefulset/ax-postgres --timeout=300s
  render ax-server.yaml | k apply -f -
  k -n "${AX_SYSTEM_NAMESPACE}" rollout status deployment/ax-server --timeout=180s
}

write_env() {
  cat > "${ENV_FILE}" << EOF
export KUBECONFIG="${KUBECONFIG}"
export KUBE_CONTEXT="${KUBE_CONTEXT}"
export AX_SYSTEM_NAMESPACE="${AX_SYSTEM_NAMESPACE}"
export AX_BIN="${AX_BIN}"
export AX_DIR="${AX_DIR}"
export SUBSTRATE_COMMIT="${SUBSTRATE_COMMIT}"
EOF
}

deploy() {
  need kubectl docker go git curl openssl
  local var
  for var in SERVER RUNNER CLI; do
    case "${!var}" in rax | ax) ;; *) die "${var} must be rax or ax, not ${!var}" ;; esac
  done
  if [[ "${CLI}" == "rax" ]]; then need cargo; fi
  check_cluster
  NODE_ARCH="$(k get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')"
  [[ "$(k get nodes -o name | wc -l)" -eq 1 ]] || warn "more than one node: the local registry only serves its own node"
  resolve_ax_dir
  resolve_substrate_dir
  deploy_registry
  deploy_substrate
  deploy_workers
  build_images
  deploy_control_plane
  write_env
  log "AX is running on ${DISTRO} with ${SERVER}-server and the ${RUNNER} task-runner. Try:"
  cat >&2 << EOF
  just ax ${DISTRO} apply -f ${AX_DIR}/examples/task.yaml
  just ax ${DISTRO} get tasks
  just status ${DISTRO}
  source ${ENV_FILE}   # to use kubectl and \$AX_BIN directly
EOF
}

## --------------------------------------
## status, ax, e2e, down, uninstall
## --------------------------------------

load_env() {
  [[ -f "${ENV_FILE}" ]] || die "${ENV_FILE} not found; run \`just deploy ${DISTRO}\` first"
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
}

status() {
  k -n ate-system get deployments,daemonsets 2> /dev/null || true
  k -n "${AX_SYSTEM_NAMESPACE}" get deployments,statefulsets,pods 2> /dev/null || true
  k -n ax-registry get pods 2> /dev/null || true
}

# node_sh runs a shell command as root on the node: inside the minikube node,
# or on the host, which is MicroK8s's node.
node_sh() {
  case "${DISTRO}" in
    minikube) minikube ssh -p "${MINIKUBE_PROFILE}" -- "sudo sh -c '$1'" ;;
    microk8s) sudo sh -c "$1" ;;
  esac
}

# human prints a byte count in MiB or GiB.
human() {
  awk -v b="$1" 'BEGIN { if (b >= 1073741824) printf "%.2f GiB", b / 1073741824; else printf "%.0f MiB", b / 1048576 }'
}

# footprint reports what the deployment costs: the node's CPU and memory
# (requested, and used when a metrics API is installed), the size of every
# image the pods run, grouped by namespace, the disk taken on the node, and
# what the builds left on the host. It only reads.
footprint() {
  need kubectl
  check_cluster
  local node
  node="$(k get nodes -o jsonpath='{.items[0].metadata.name}')"

  log "Node ${node}: capacity and what the pods request"
  k get node "${node}" -o jsonpath='  capacity:    cpu={.status.capacity.cpu} memory={.status.capacity.memory} ephemeral-storage={.status.capacity.ephemeral-storage}{"\n"}  allocatable: cpu={.status.allocatable.cpu} memory={.status.allocatable.memory}{"\n"}'
  k describe node "${node}" | sed -n '/^Allocated resources:/,/^Events:/p' | sed '$d'

  log "CPU and memory in use"
  if k top node "${node}" 2> /dev/null; then
    k top pods -A --sort-by=memory 2> /dev/null | head -n 25 || true
  else
    echo "  no metrics API; for live usage run: minikube addons enable metrics-server -p ${MINIKUBE_PROFILE}"
    echo "  (MicroK8s: microk8s enable metrics-server), then run this again"
  fi

  log "Images the pods run (uncompressed, as the node reports them)"
  # Join each pod's image digests with the node's image list, which carries
  # the sizes. The kubelet lists at most 50 images, the largest first.
  {
    k get node "${node}" -o jsonpath='{range .status.images[*]}size {.sizeBytes} {.names[*]}{"\n"}{end}'
    k get pods -A -o jsonpath='{range .items[*]}pod {.metadata.namespace} {.status.initContainerStatuses[*].imageID} {.status.containerStatuses[*].imageID}{"\n"}{end}'
  } | awk '
    # Node image lines come first: index each image by every name and digest.
    $1 == "size" {
      label = $3
      for (i = 3; i <= NF; i++) if ($i !~ /@sha256:/) { label = $i; break }
      for (i = 3; i <= NF; i++) { d = $i; sub(/^.*@/, "", d); size[$i] = size[d] = $2; tag[$i] = tag[d] = label }
      next
    }
    $1 == "pod" {
      for (i = 3; i <= NF; i++) {
        id = $i; sub(/^docker-pullable:\/\//, "", id)
        if (!(id in size)) sub(/^.*@/, "", id)
        if (!(id in size) || shown[tag[id]]++) continue
        printf "  %-34s %7.0f MiB  %s\n", $2, size[id] / 1048576, tag[id]
        total[$2] += size[id]; all += size[id]
      }
    }
    END {
      fflush()
      for (ns in total) printf "  %-34s %7.0f MiB  total\n", ns, total[ns] / 1048576 | "sort"
      close("sort")
      printf "  %-34s %7.0f MiB\n", "every image in use", all / 1048576
    }
  '
  echo "  all images on the node: $(human "$(k get node "${node}" -o jsonpath='{range .status.images[*]}{.sizeBytes}{"\n"}{end}' | awk '{ t += $1 } END { print t + 0 }')")"

  log "Disk on the node"
  case "${DISTRO}" in
    minikube)
      node_sh 'df -h /var | tail -n 1; du -sh /var/lib/containerd /var/lib/ax-registry /tmp/hostpath-provisioner /var/lib/kubelet 2>/dev/null; true'
      ;;
    microk8s)
      node_sh 'df -h /var/snap/microk8s/common | tail -n 1; du -sh /var/snap/microk8s/common/var/lib/containerd /var/lib/ax-registry /var/snap/microk8s/common/default-storage /var/snap/microk8s/common/var/lib/kubelet 2>/dev/null; true'
      ;;
  esac
  k get pvc -A 2> /dev/null || true

  log "On the host (builds and caches)"
  if [[ "${DISTRO}" == minikube ]]; then
    echo "  minikube node container and base image:"
    docker ps -a --size --filter "name=^${MINIKUBE_PROFILE}$" --format '    {{.Names}}  {{.Size}}' 2> /dev/null || true
  fi
  docker system df 2> /dev/null || true
  du -sh "${TMP_DIR}" "${REPO_ROOT}/target" 2> /dev/null || true
}

run_ax() {
  load_env
  exec "${AX_BIN}" "$@"
}

run_e2e() {
  load_env
  # Explicit variables win over AX's .tmp/e2e.env, so this does not disturb
  # a kind setup in the same AX checkout.
  cd "${AX_DIR}"
  exec go test -v -count=1 -timeout "${E2E_TIMEOUT:-30m}" ./test/e2e/... -args --e2e "$@"
}

down() {
  case "${DISTRO}" in
    minikube)
      need minikube
      log "Deleting minikube profile '${MINIKUBE_PROFILE}'..."
      minikube delete -p "${MINIKUBE_PROFILE}"
      ;;
    microk8s)
      log "Removing AX, Substrate and the registry from MicroK8s (MicroK8s itself stays)..."
      k delete namespace "${AX_SYSTEM_NAMESPACE}" --ignore-not-found --wait=false || true
      uninstall_substrate
      k delete clusterrole,clusterrolebinding ax-server --ignore-not-found || true
      k delete namespace ax-registry --ignore-not-found || true
      warn "registry images stay in /var/lib/ax-registry on this host; remove the directory to reclaim the space"
      ;;
  esac
  rm -f "${ENV_FILE}" "${KUBECONFIG}"
}

uninstall() {
  case "${DISTRO}" in
    minikube) down ;;
    microk8s)
      log "Removing the MicroK8s snap and all its data (needs sudo)..."
      sudo snap remove microk8s --purge
      if [[ -L /var/lib/kubelet ]]; then
        sudo rm /var/lib/kubelet
      fi
      rm -f "${ENV_FILE}" "${KUBECONFIG}"
      ;;
  esac
}

case "${COMMAND}" in
  install) "install_${DISTRO}" ;;
  up) "up_${DISTRO}" ;;
  deploy) deploy ;;
  status) status ;;
  footprint) footprint ;;
  ax) run_ax "$@" ;;
  e2e) run_e2e "$@" ;;
  down) down ;;
  uninstall) uninstall ;;
  *) usage ;;
esac
