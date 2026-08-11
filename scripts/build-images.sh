#!/usr/bin/env bash
# Builds both application images and, optionally, loads them into a local cluster.
#
#   ./scripts/build-images.sh                  # build only
#   ./scripts/build-images.sh --load kind      # build, then load into kind
#   ./scripts/build-images.sh --load minikube  # build, then load into minikube
#
# Docker Desktop with Kubernetes enabled needs no load step: it shares one image store.
set -euo pipefail

TAG="${IMAGE_TAG:-dev}"
BACKEND="redepanda-backend:${TAG}"
FRONTEND="redepanda-frontend:${TAG}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LOAD_INTO=""
CLUSTER_NAME="${CLUSTER_NAME:-kind}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --load) LOAD_INTO="${2:-}"; shift 2 ;;
        -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Prefer podman when both are present: on this project's dev machines `docker` is often a
# podman shim anyway, and being explicit avoids surprises about which store the image lands in.
if command -v podman >/dev/null 2>&1; then
    ENGINE=podman
elif command -v docker >/dev/null 2>&1; then
    ENGINE=docker
else
    echo "Neither podman nor docker found on PATH." >&2
    exit 1
fi

echo "==> Building with ${ENGINE}"
# The backend's context is the repository root because it references RedePanda.Contracts.
"${ENGINE}" build -f "${REPO_ROOT}/src/RedePanda.Backend/Dockerfile" -t "${BACKEND}" "${REPO_ROOT}"
"${ENGINE}" build -t "${FRONTEND}" "${REPO_ROOT}/src/RedePanda.Frontend"

echo "==> Built ${BACKEND} and ${FRONTEND}"

[[ -z "${LOAD_INTO}" ]] && exit 0

case "${LOAD_INTO}" in
    kind)
        if [[ "${ENGINE}" == "podman" ]]; then
            # `kind load docker-image` reads the *Docker* store, which podman does not populate.
            # Going through an archive is the supported route for a podman-built image.
            echo "==> Loading into kind cluster '${CLUSTER_NAME}' via image archive"
            TMP="$(mktemp -d)"
            trap 'rm -rf "${TMP}"' EXIT
            podman save -o "${TMP}/backend.tar" "${BACKEND}"
            podman save -o "${TMP}/frontend.tar" "${FRONTEND}"
            kind load image-archive "${TMP}/backend.tar" --name "${CLUSTER_NAME}"
            kind load image-archive "${TMP}/frontend.tar" --name "${CLUSTER_NAME}"
        else
            kind load docker-image "${BACKEND}" "${FRONTEND}" --name "${CLUSTER_NAME}"
        fi
        ;;
    minikube)
        echo "==> Loading into minikube profile '${MINIKUBE_PROFILE}'"
        minikube image load "${BACKEND}" "${FRONTEND}" -p "${MINIKUBE_PROFILE}"
        ;;
    *)
        echo "Unknown --load target '${LOAD_INTO}'. Use 'kind' or 'minikube'." >&2
        exit 2
        ;;
esac

echo "==> Images are available inside the cluster"
