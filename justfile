# Recipes for developing RAX and running AX on a local cluster.
# `just` lists them. The local-cluster recipes take a distribution,
# `minikube` or `microk8s`; see docs/local-kubernetes.md.

local := justfile_directory() / "hack/local/local.sh"

# List the recipes.
default:
    @just --list

# Run the unit and integration tests.
test:
    cargo test --all

# Run clippy and check formatting, as CI does.
lint:
    cargo fmt --all --check
    cargo clippy --all-targets -- -D warnings

# Build the rax-server image (the one `deploy` pushes to the local cluster).
image tag="rax-server:local":
    docker build -f Dockerfile.server -t {{tag}} .

# Build the task-runner image, a drop-in for AX's ax-task-runner image.
runner-image tag="rax-task-runner:local":
    docker build -f Dockerfile.task-runner -t {{tag}} .

# Build the Claude Code task image on top of the task-runner image (AX's `make build-claude-code-image`). Pin a Claude Code release with CLAUDE_CODE_VERSION.
claude-code-image tag="rax-claude-code:local": (runner-image)
    docker build -f Dockerfile.claude-code --build-arg CLAUDE_CODE_VERSION=${CLAUDE_CODE_VERSION:-latest} -t {{tag}} .

# Replay AX's end-to-end scenarios without Kubernetes. Point RAX_E2E_CLI, RAX_E2E_RUNNER or RAX_E2E_SERVER at AX's Go binaries to mix implementations.
e2e-local *args:
    cargo test -p rax-cli --test e2e_local {{args}}

# Install a light Kubernetes distribution (minikube or microk8s) and kubectl.
install distro:
    {{local}} install {{distro}}

# Create (minikube) or configure (microk8s) a single-node cluster with the feature gates Substrate needs.
up distro:
    {{local}} up {{distro}}

# Build Substrate, AX's task-runner and the control plane from source and deploy them; re-run after changing code. SERVER=ax deploys AX's Go server instead of rax-server.
deploy distro:
    {{local}} deploy {{distro}}

# Install, create and deploy in one go.
all distro: (install distro) (up distro) (deploy distro)

# Show the Substrate, AX and registry workloads.
status distro:
    {{local}} status {{distro}}

# Report what the deployment costs: CPU and memory requested and used, image sizes by namespace, disk on the node and on the host.
footprint distro:
    {{local}} footprint {{distro}}

# Run the ax CLI against the cluster, e.g. `just ax minikube get tasks`.
ax distro *args:
    {{local}} ax {{distro}} {{args}}

# Run AX's end-to-end suite against the cluster, e.g. `just e2e minikube -test.run TestE2E_ModelCRUD`.
e2e distro *args:
    {{local}} e2e {{distro}} {{args}}

# Delete the minikube cluster, or remove AX and Substrate from MicroK8s.
down distro:
    {{local}} down {{distro}}

# Remove MicroK8s entirely (snap remove --purge); for minikube, the same as down.
uninstall distro:
    {{local}} uninstall {{distro}}

# Shortcut: minikube from scratch.
minikube: (all "minikube")

# Shortcut: MicroK8s from scratch.
microk8s: (all "microk8s")

# Write the README drawing (docs/drawings/cosmos.svg and its Desmos state) from drawings/*.nix.
drawing:
    art=$(nix build .#cosmos-art --no-link --print-out-paths) && \
    cat "$art/cosmos.svg" > docs/drawings/cosmos.svg && \
    cat "$art/cosmos.desmos.json" > docs/drawings/cosmos.desmos.json
