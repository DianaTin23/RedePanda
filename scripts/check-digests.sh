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

if [[ -z "${CONTAINERS_REGISTRIES_CONF:-}" ]]; then
    probe_output="$(skopeo inspect --raw docker://127.0.0.1:1/probe:latest 2>&1 || true)"
    if [[ "${probe_output}" == *"registries configuration"* ]]; then
        TMP_CONF="$(mktemp -d)"
        trap 'rm -rf "${TMP_CONF}"' EXIT
        printf 'unqualified-search-registries = ["docker.io"]\n' > "${TMP_CONF}/registries.conf"
        export CONTAINERS_REGISTRIES_CONF="${TMP_CONF}/registries.conf"
    fi
fi

collect_pins() {
    grep -rhoE '[a-zA-Z0-9._/-]+:[a-zA-Z0-9._-]+@sha256:[0-9a-f]{64}' \
        "${REPO_ROOT}/src/RedeTim.Backend/Dockerfile" \
        "${REPO_ROOT}/src/RedeTim.ChatClient/Dockerfile" \
        "${REPO_ROOT}/src/RedeTim.Frontend/Dockerfile" \
        "${REPO_ROOT}/deploy/helm/redetim/values.yaml" \
        "${REPO_ROOT}/RedeTim-kafka-docker/docker-compose.yml" \
        "${REPO_ROOT}/RedeTim-kafka-docker/make-tls.sh" \
    | sort -u
}

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

broker_ref() {
    grep -oE 'redpandadata/redpanda:[a-zA-Z0-9._-]+@sha256:[0-9a-f]{64}' "$1" | head -1 || true
}

BROKER_SOURCES=(
    "RedeTim-kafka-docker/docker-compose.yml"
    "RedeTim-kafka-docker/make-tls.sh"
)

CHART_BROKER="$(broker_ref "${REPO_ROOT}/deploy/helm/redetim/values.yaml")"

if [[ -z "${CHART_BROKER}" ]]; then
    echo "?? broker parity: no pin found in the chart, so nothing could be compared against it"
    drifted=1
else
    for source in "${BROKER_SOURCES[@]}"; do
        source_broker="$(broker_ref "${REPO_ROOT}/${source}")"
        if [[ -z "${source_broker}" ]]; then
            echo "?? broker parity: no pin found in ${source}, so it went unchecked"
            drifted=1
        elif [[ "${source_broker}" != "${CHART_BROKER}" ]]; then
            echo "DRIFT broker parity: ${source} and the chart name different brokers"
            echo "     ${source}: ${source_broker}"
            echo "     chart:     ${CHART_BROKER}"
            drifted=1
        else
            echo "ok broker parity: ${source}"
        fi
    done
fi

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
