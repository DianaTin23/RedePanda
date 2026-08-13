#!/usr/bin/env bash
# Builds both application images under an immutable version tag and writes the release file
# that deploys them. Optionally loads the images into a local cluster.
#
#   ./scripts/build-images.sh                  # build only
#   ./scripts/build-images.sh --load kind      # build, then load into kind
#   ./scripts/build-images.sh --load minikube  # build, then load into minikube
#   ./scripts/build-images.sh --release        # refuse to build from an unclean working tree
#
# Docker Desktop with Kubernetes enabled needs no load step: it shares one image store.
#
# The tag is derived, never chosen: <appVersion from Chart.yaml>-g<short sha>, e.g.
# 0.1.0-g103b98b. It is never reused, so one tag names exactly one build -- which is what makes
# `helm rollback` restore an actual image rather than the same mutable name it started from.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${REPO_ROOT}/deploy/helm/redepanda"
RELEASE_DIR="${REPO_ROOT}/deploy/releases"

LOAD_INTO=""
REQUIRE_CLEAN=0
CLUSTER_NAME="${CLUSTER_NAME:-kind}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --load) LOAD_INTO="${2:-}"; shift 2 ;;
        --release) REQUIRE_CLEAN=1; shift ;;
        -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# ---- Version --------------------------------------------------------------------------------

# appVersion is quoted in Chart.yaml; the pattern tolerates it either way.
APP_VERSION="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}[[:space:]]*$/\1/p' \
    "${CHART_DIR}/Chart.yaml")"
if [[ -z "${APP_VERSION}" ]]; then
    echo "Could not read appVersion from ${CHART_DIR}/Chart.yaml" >&2
    exit 2
fi

if ! git -C "${REPO_ROOT}" rev-parse --git-dir >/dev/null 2>&1; then
    echo "Not a git repository, so no commit can identify this build." >&2
    echo "Set IMAGE_TAG=<something> to build anyway; the result is not a release." >&2
    exit 2
fi

GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
VERSION="${APP_VERSION}-g$(git -C "${REPO_ROOT}" rev-parse --short=7 HEAD)"
DIRTY=false

if [[ -n "$(git -C "${REPO_ROOT}" status --porcelain)" ]]; then
    DIRTY=true
    # A plain "-dirty" suffix would be mutable all over again: every edit would land under the
    # same tag. Hashing the uncommitted content instead keeps one tag to one tree. `git diff
    # HEAD` covers modifications and deletions; the file list adds the contents of untracked
    # files, which no diff against HEAD can see.
    #
    # `-o` and not `-mo`: a deleted tracked file is "modified" to git, so -m listed a path that
    # sha256sum then could not open, and xargs exited 123 -- killing the whole build under
    # `set -e` before anything was built. Deletions are already in the diff above.
    DIRTY_HASH="$({
        git -C "${REPO_ROOT}" diff HEAD
        git -C "${REPO_ROOT}" ls-files -o --exclude-standard -z \
            | (cd "${REPO_ROOT}" && xargs -0 -r sha256sum)
    } | sha256sum | cut -c1-7)"
    VERSION="${VERSION}-dirty.${DIRTY_HASH}"
fi

# Escape hatch, for building outside the release path entirely (a scratch image to poke at, a
# tag someone else's tooling expects). It bypasses the version derivation, so say so.
if [[ -n "${IMAGE_TAG:-}" ]]; then
    echo "!! IMAGE_TAG is set: building '${IMAGE_TAG}' instead of '${VERSION}'."
    echo "!! No release file is written and the result identifies no commit."
    VERSION="${IMAGE_TAG}"
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

BACKEND="redepanda-backend:${VERSION}"
FRONTEND="redepanda-frontend:${VERSION}"

# The console client, and the image the chart's topic Job runs with --ensure-topic. It carries one
# tag with the other two on purpose: the admin process must be the same build as the application
# it administers, which is the whole point of it no longer being a shell script in a foreign image.
CHATCLIENT="redepanda-chatclient:${VERSION}"

# ---- Build ----------------------------------------------------------------------------------

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

echo "==> Building ${VERSION} with ${ENGINE}"
# The backend and the chat client build from the repository root because both reference
# RedePanda.Contracts. Only the frontend has a context of its own.
"${ENGINE}" build -f "${REPO_ROOT}/src/RedePanda.Backend/Dockerfile" -t "${BACKEND}" "${REPO_ROOT}"
"${ENGINE}" build -f "${REPO_ROOT}/src/RedePanda.ChatClient/Dockerfile" -t "${CHATCLIENT}" "${REPO_ROOT}"
"${ENGINE}" build -t "${FRONTEND}" "${REPO_ROOT}/src/RedePanda.Frontend"

echo "==> Built ${BACKEND}, ${CHATCLIENT} and ${FRONTEND}"

# ---- Release file ---------------------------------------------------------------------------

RELEASE_FILE=""
if [[ -z "${IMAGE_TAG:-}" ]]; then
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
fi

# There is deliberately no rendered-manifest step here any more. deploy/k8s/rendered.yaml used to
# be written from `helm template` so the release could also be installed without Helm, and it cost
# more than it paid for:
#
#   * a rendered file cannot carry the chart's render-time `fail` guards, so the path that skipped
#     Helm also skipped every check that makes a misconfiguration loud;
#   * TLS has no off switch, so every render minted a certificate authority and four private keys
#     and committed them here;
#   * `helm template` always renders .Release.Revision as 1, so the topic Job kept one name and the
#     second `kubectl apply` failed on an immutable field;
#   * nothing detected drift, and it drifted -- by five missing Secrets and several hundred lines.
#
# Helm is the install path. The release artifact is deploy/releases/<version>.yaml, which is what
# pins the immutable image tag and therefore what makes `helm rollback` restore a real build.

# ---- Load into a local cluster ---------------------------------------------------------------

if [[ -n "${LOAD_INTO}" ]]; then
    case "${LOAD_INTO}" in
        kind)
            if [[ "${ENGINE}" == "podman" ]]; then
                # `kind load docker-image` reads the *Docker* store, which podman does not
                # populate. Going through an archive is the supported route for a podman-built
                # image.
                #
                # The retag is not cosmetic. Podman stores a locally built image under
                # `localhost/<name>`, and `podman save` writes that name into the archive, so the
                # node ends up holding `localhost/redepanda-backend:<tag>`. The chart asks for the
                # bare `redepanda-backend:<tag>`, which containerd normalises to
                # `docker.io/library/redepanda-backend:<tag>` -- a name the archive never carried.
                # The kubelet then does the only thing left to it and tries to pull from Docker
                # Hub, which fails with ImagePullBackOff on an image that is demonstrably already
                # on the node. Saving under the fully qualified name is what makes the two agree.
                # `docker save` adds no prefix, so the branch below needs none of this.
                echo "==> Loading into kind cluster '${CLUSTER_NAME}' via image archive"
                TMP="$(mktemp -d)"
                trap 'rm -rf "${TMP}"' EXIT
                for image in "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}"; do
                    podman tag "${image}" "docker.io/library/${image}"
                done
                podman save -o "${TMP}/backend.tar" "docker.io/library/${BACKEND}"
                podman save -o "${TMP}/chatclient.tar" "docker.io/library/${CHATCLIENT}"
                podman save -o "${TMP}/frontend.tar" "docker.io/library/${FRONTEND}"
                kind load image-archive "${TMP}/backend.tar" --name "${CLUSTER_NAME}"
                kind load image-archive "${TMP}/chatclient.tar" --name "${CLUSTER_NAME}"
                kind load image-archive "${TMP}/frontend.tar" --name "${CLUSTER_NAME}"
            else
                kind load docker-image "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}" --name "${CLUSTER_NAME}"
            fi
            ;;
        minikube)
            echo "==> Loading into minikube profile '${MINIKUBE_PROFILE}'"
            if [[ "${ENGINE}" == "podman" ]]; then
                # Same `localhost/` prefix problem as the kind branch above, for the same reason:
                # the name podman stores is the name minikube carries into the cluster, and the
                # chart asks for the normalised one. Unlike the kind path this has not been
                # exercised on a real cluster here -- there is no minikube on the dev machines --
                # so it is the identical fix applied to an identical mechanism, not a tested one.
                for image in "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}"; do
                    podman tag "${image}" "docker.io/library/${image}"
                done
                minikube image load "docker.io/library/${BACKEND}" "docker.io/library/${CHATCLIENT}" \
                    "docker.io/library/${FRONTEND}" -p "${MINIKUBE_PROFILE}"
            else
                minikube image load "${BACKEND}" "${CHATCLIENT}" "${FRONTEND}" -p "${MINIKUBE_PROFILE}"
            fi
            ;;
        *)
            echo "Unknown --load target '${LOAD_INTO}'. Use 'kind' or 'minikube'." >&2
            exit 2
            ;;
    esac
    echo "==> Images are available inside the cluster"
fi

# ---- What to run next -------------------------------------------------------------------------

if [[ -n "${RELEASE_FILE}" ]]; then
    # --description is the only per-revision field helm lets a value reach: `helm history` reads
    # its APP VERSION column from Chart.yaml, which is identical on every revision and therefore
    # useless for telling two releases apart.
    cat <<EOF

Deploy this release:

  helm upgrade --install redepanda ${CHART_DIR#"${REPO_ROOT}/"} \\
    -n redepanda --create-namespace --wait --timeout 10m \\
    -f ${RELEASE_FILE#"${REPO_ROOT}/"} \\
    --description "release ${VERSION}"
EOF
fi
