#!/usr/bin/env bash
# Reports whether any digest-pinned image has moved upstream.
#
#   ./scripts/check-digests.sh          # check every pinned image
#
# Exit status: 0 when every pin still matches the tag it names, 1 when at least one has
# drifted, 2 on a usage or tooling error. Nothing is ever rewritten -- the script prints the
# replacement line and leaves the edit to a human, because this repository has no CI to catch
# a bad automated rewrite of a Dockerfile or of values.yaml.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if ! command -v skopeo >/dev/null 2>&1; then
    echo "skopeo not found on PATH. It is in the Nix dev shell: nix develop" >&2
    exit 2
fi

# skopeo refuses to start when /etc/containers/registries.conf is in the old v1 format, which
# is what some distributions (NixOS among them) still install for podman. None of the lookups
# below rely on registry search paths -- normalise_ref qualifies every reference itself -- so
# pointing skopeo at a minimal v2 file sidesteps the incompatibility without changing any
# result. The probe deliberately targets a closed local port: it fails either way, and we only
# care which error comes back, so it costs no registry request.
if [[ -z "${CONTAINERS_REGISTRIES_CONF:-}" ]]; then
    # The probe is expected to fail, so its status is discarded and only its message is read;
    # piping it into grep instead would trip pipefail and take the whole script down.
    probe_output="$(skopeo inspect --raw docker://127.0.0.1:1/probe:latest 2>&1 || true)"
    if [[ "${probe_output}" == *"registries configuration"* ]]; then
        TMP_CONF="$(mktemp -d)"
        trap 'rm -rf "${TMP_CONF}"' EXIT
        printf 'unqualified-search-registries = ["docker.io"]\n' > "${TMP_CONF}/registries.conf"
        export CONTAINERS_REGISTRIES_CONF="${TMP_CONF}/registries.conf"
    fi
fi

# Every digest-pinned reference in the repository, one "image:tag@sha256:..." per line. The
# file list is spelled out rather than globbed: a new pin in a file nobody added here goes
# unchecked, which is visible, instead of some unrelated file being scraped for sha256 strings.
collect_pins() {
    grep -rhoE '[a-zA-Z0-9._/-]+:[a-zA-Z0-9._-]+@sha256:[0-9a-f]{64}' \
        "${REPO_ROOT}/src/RedePanda.Backend/Dockerfile" \
        "${REPO_ROOT}/src/RedePanda.ChatClient/Dockerfile" \
        "${REPO_ROOT}/src/RedePanda.Frontend/Dockerfile" \
        "${REPO_ROOT}/deploy/helm/redepanda/values.yaml" \
        "${REPO_ROOT}/RedePanda-kafka-docker/docker-compose.yml" \
    | sort -u
}

# `alpine:3.23` and `docker.io/library/alpine:3.23` name the same image, but only the second
# form is unambiguous to skopeo, which has no notion of the daemon's default namespace. Apply
# the same two rules a container engine applies: a name with no slash lives in
# docker.io/library, and a single-segment prefix that is not a hostname (no dot, no port) is a
# Docker Hub user rather than a registry.
normalise_ref() {
    case "$1" in
        */*) ;;
        *) echo "docker.io/library/$1"; return ;;
    esac
    case "${1%%/*}" in
        *.*|*:*|localhost) echo "$1" ;;
        *) echo "docker.io/$1" ;;
    esac
}

# The digest of a tag is the sha256 of the manifest bytes the registry returns for it. For a
# multi-architecture image that manifest is the index, so hashing it here yields the same
# index digest that a `docker pull` on any architecture resolves through. Asking skopeo for a
# platform-specific manifest instead would produce a pin that only works on amd64.
#
# A lookup can legitimately fail -- no network, or a registry rate-limiting anonymous reads --
# and that has to be reportable rather than fatal, so the failure is swallowed here and the
# caller treats empty output as "unverified". Hashing the empty string would otherwise yield a
# perfectly valid-looking digest that matches nothing.
resolve_digest() {
    local raw
    raw="$(skopeo inspect --raw "docker://$1" 2>/dev/null || true)"
    if [[ -z "${raw}" ]]; then
        return 0
    fi
    printf '%s' "${raw}" | sha256sum | cut -d' ' -f1
}

drifted=0
checked=0

# ---- Broker parity ---------------------------------------------------------------------------

# Dev/prod parity, enforced instead of asserted. The broker in the compose file and the broker in
# the chart are meant to be the same image down to the digest -- that is the whole of what parity
# means for this repository's one shared backing service, and a comment claiming it is not a
# check. Two people updating one file and not the other is the ordinary way it breaks.
#
# Runs before the registry lookups because it needs no network: an offline run still gets this
# answer, and a mismatch here would make the drift report below misleading anyway.
broker_ref() {
    grep -oE 'redpandadata/redpanda:[a-zA-Z0-9._-]+@sha256:[0-9a-f]{64}' "$1" | head -1 || true
}

COMPOSE_BROKER="$(broker_ref "${REPO_ROOT}/RedePanda-kafka-docker/docker-compose.yml")"
CHART_BROKER="$(broker_ref "${REPO_ROOT}/deploy/helm/redepanda/values.yaml")"

if [[ -z "${COMPOSE_BROKER}" || -z "${CHART_BROKER}" ]]; then
    echo "?? broker parity: could not read the pin from both files, so it went unchecked"
    drifted=1
elif [[ "${COMPOSE_BROKER}" != "${CHART_BROKER}" ]]; then
    echo "DRIFT broker parity: local and cluster run different brokers"
    echo "     compose: ${COMPOSE_BROKER}"
    echo "     chart:   ${CHART_BROKER}"
    drifted=1
else
    echo "ok broker parity: ${COMPOSE_BROKER}"
fi

# ---- Registry lookups ------------------------------------------------------------------------

while IFS= read -r pin; do
    [[ -z "${pin}" ]] && continue
    ref="${pin%@*}"
    pinned="${pin#*@}"
    checked=$((checked + 1))

    hash="$(resolve_digest "$(normalise_ref "${ref}")")"

    if [[ -z "${hash}" ]]; then
        echo "?? ${ref}"
        echo "     could not read the manifest (network, auth, or rate limit); left unchecked"
        drifted=1
        continue
    fi
    current="sha256:${hash}"

    if [[ "${current}" == "${pinned}" ]]; then
        echo "ok ${ref}"
    else
        echo "DRIFT ${ref}"
        echo "     pinned:  ${pinned}"
        echo "     current: ${current}"
        echo "     replace with: ${ref}@${current}"
        drifted=1
    fi
done < <(collect_pins)

echo
if [[ "${drifted}" -eq 0 ]]; then
    echo "==> ${checked} pinned images, all current"
else
    echo "==> Some pins are stale or unverifiable. Update the file, rebuild, and re-run."
    echo "    A broker-parity mismatch is fixed by hand in whichever of the two files is behind;"
    echo "    both must name the same digest."
fi
exit "${drifted}"
