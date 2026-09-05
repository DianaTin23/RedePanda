#!/usr/bin/env bash
# Builds the three application images under an immutable version tag and writes the release
# file that deploys them. Optionally loads them into a local cluster, or pushes them.
#
#   ./scripts/build-images.sh                  # build only
#   ./scripts/build-images.sh --load kind      # build, then load into kind
#   ./scripts/build-images.sh --load minikube  # build, then load into minikube
#   ./scripts/build-images.sh --release        # refuse to build from an unclean working tree
#   ./scripts/build-images.sh --push           # implies --release, then push to the registry
#
# Docker Desktop with Kubernetes enabled needs no load step: it shares one image store.
#
# The image names are read from the chart's values.yaml, so this cannot build under a name the
# chart does not deploy. Set ENGINE=docker|podman to override the engine probe.
#
# The tag is derived, never chosen: <appVersion from Chart.yaml>-g<short sha>, e.g.
# 0.1.0-g103b98b. It is never reused, so one tag names exactly one build -- which is what makes
# `helm rollback` restore an actual image rather than the same mutable name it started from.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
REPO_ROOT="$(repo_root)"
CHART_DIR="${REPO_ROOT}/deploy/helm/redetim"
RELEASE_DIR="${REPO_ROOT}/deploy/releases"

LOAD_INTO=""
REQUIRE_CLEAN=0
PUSH=0
CLUSTER_NAME="${CLUSTER_NAME:-kind}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --load) LOAD_INTO="${2:-}"; shift 2 ;;
        --release) REQUIRE_CLEAN=1; shift ;;
        --push) PUSH=1; REQUIRE_CLEAN=1; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

APP_VERSION="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}[[:space:]]*$/\1/p' \
    "${CHART_DIR}/Chart.yaml")"
if [[ -z "${APP_VERSION}" ]]; then
    echo "Could not read appVersion from ${CHART_DIR}/Chart.yaml" >&2
    exit 2
fi

# The chart names the images; this script only builds what the chart will deploy. Reading the
# names here rather than repeating them is what keeps the two from drifting apart.
image_repo() {
    local component="$1" repo
    repo="$(sed -n "/^${component}:/,/^[a-zA-Z]/p" "${CHART_DIR}/values.yaml" \
        | sed -n 's/^[[:space:]]*repository:[[:space:]]*\(.*\)$/\1/p' | head -1)"
    if [[ -z "${repo}" ]]; then
        echo "Could not read ${component}.image.repository from ${CHART_DIR}/values.yaml" >&2
        exit 2
    fi
    printf '%s' "${repo}"
}

if ! git -C "${REPO_ROOT}" rev-parse --git-dir >/dev/null 2>&1; then
    echo "Not a git repository, so no commit can identify this build." >&2
    echo "The tag is derived from the commit; there is nothing to derive it from here." >&2
    exit 2
fi

GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
VERSION="${APP_VERSION}-g$(git -C "${REPO_ROOT}" rev-parse --short=7 HEAD)"
DIRTY=false

if [[ -n "$(git -C "${REPO_ROOT}" status --porcelain)" ]]; then
    DIRTY=true
    DIRTY_HASH="$({
        git -C "${REPO_ROOT}" diff HEAD
        git -C "${REPO_ROOT}" ls-files -o --exclude-standard -z \
            | (cd "${REPO_ROOT}" && xargs -0 -r sha256sum)
    } | sha256sum | cut -c1-7)"
    VERSION="${VERSION}-dirty.${DIRTY_HASH}"
fi

if [[ "${DIRTY}" == true ]]; then
    if [[ "${REQUIRE_CLEAN}" -eq 1 ]]; then
        echo "Refusing to cut a release from an unclean working tree." >&2
        echo "Commit or stash first, then re-run. Without --release this builds anyway." >&2
        git -C "${REPO_ROOT}" status --short >&2
        exit 1
    fi
    echo "!! Working tree is dirty -- this build is NOT a reproducible release."
    echo "!! The tag names the uncommitted content, but nothing in git does."
fi

BACKEND="$(image_repo backend):${VERSION}"
FRONTEND="$(image_repo frontend):${VERSION}"
CHATCLIENT="$(image_repo chatClient):${VERSION}"

# podman first, because on these machines `docker` is often a podman shim. CI sets ENGINE
# explicitly: a runner has both, and the two read their registry credentials from different
# files, so letting the probe decide turns a push failure into an opaque 'unauthorized'.
if [[ -n "${ENGINE:-}" ]]; then
    if ! command -v "${ENGINE}" >/dev/null 2>&1; then
        echo "ENGINE=${ENGINE} is not on PATH." >&2
        exit 2
    fi
elif command -v podman >/dev/null 2>&1; then
    ENGINE=podman
elif command -v docker >/dev/null 2>&1; then
    ENGINE=docker
else
    echo "Neither podman nor docker found on PATH." >&2
    exit 1
fi

echo "==> Building ${VERSION} with ${ENGINE}"
"${ENGINE}" build -f "${REPO_ROOT}/src/RedeTim.Backend/Dockerfile" -t "${BACKEND}" "${REPO_ROOT}"
"${ENGINE}" build -f "${REPO_ROOT}/src/RedeTim.ChatClient/Dockerfile" -t "${CHATCLIENT}" "${REPO_ROOT}"
"${ENGINE}" build -t "${FRONTEND}" "${REPO_ROOT}/src/RedeTim.Frontend"

echo "==> Built ${BACKEND}, ${CHATCLIENT} and ${FRONTEND}"

if [[ "${PUSH}" -eq 1 ]]; then
    for image in "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}"; do
        echo "==> Pushing ${image}"
        "${ENGINE}" push "${image}"
    done
fi

mkdir -p "${RELEASE_DIR}"
RELEASE_FILE="${RELEASE_DIR}/${VERSION}.yaml"
cat > "${RELEASE_FILE}" <<EOF
# Generated by scripts/build-images.sh -- do not edit.
#
# This file is the release: it names the exact images a build produced. Passing it to helm is
# what binds that build to the chart's configuration, and it is what a later \`helm rollback\`
# restores. Commit it, unless the version says dirty.
release:
  version: "${VERSION}"
  gitSha: "${GIT_SHA}"
  builtAt: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  dirty: ${DIRTY}

backend:
  image:
    tag: "${VERSION}"
frontend:
  image:
    tag: "${VERSION}"
chatClient:
  image:
    tag: "${VERSION}"
EOF
echo "==> Wrote ${RELEASE_FILE#"${REPO_ROOT}/"}"

if [[ -n "${LOAD_INTO}" ]]; then
    case "${LOAD_INTO}" in
        kind)
            if [[ "${ENGINE}" == "podman" ]]; then
                echo "==> Loading into kind cluster '${CLUSTER_NAME}' via image archive"
                TMP="$(mktemp -d)"
                trap 'rm -rf "${TMP}"' EXIT
                podman save -o "${TMP}/backend.tar" "${BACKEND}"
                podman save -o "${TMP}/chatclient.tar" "${CHATCLIENT}"
                podman save -o "${TMP}/frontend.tar" "${FRONTEND}"
                kind load image-archive "${TMP}/backend.tar" --name "${CLUSTER_NAME}"
                kind load image-archive "${TMP}/chatclient.tar" --name "${CLUSTER_NAME}"
                kind load image-archive "${TMP}/frontend.tar" --name "${CLUSTER_NAME}"
            else
                kind load docker-image "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}" --name "${CLUSTER_NAME}"
            fi
            ;;
        minikube)
            echo "==> Loading into minikube profile '${MINIKUBE_PROFILE}'"
            minikube image load "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}" -p "${MINIKUBE_PROFILE}"
            ;;
        *)
            echo "Unknown --load target '${LOAD_INTO}'. Use 'kind' or 'minikube'." >&2
            exit 2
            ;;
    esac
    echo "==> Images are available inside the cluster"
fi

cat <<EOF

Deploy this release:

  helm upgrade --install redetim ${CHART_DIR#"${REPO_ROOT}/"} \\
    -n redetim --create-namespace --wait --timeout 10m \\
    -f ${RELEASE_FILE#"${REPO_ROOT}/"} \\
    --description "release ${VERSION}"
EOF
