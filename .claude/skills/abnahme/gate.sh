#!/usr/bin/env bash
# The local gate: everything dotnet.yml and chart.yml check, run in one go before pushing.
#
#   .claude/skills/abnahme/gate.sh              # everything
#   .claude/skills/abnahme/gate.sh --chart-only # skip the .NET half (slow part)
#
# Exit status: 0 when every step passed, 1 when one of them failed, 2 on a usage or tooling error.
#
# Three of these steps are easy to leave out by hand, and each of them is the whole point of the
# step it belongs to:
#   * the HPA variant has to be rendered *separately*, or nothing ever validates backend-hpa.yaml;
#   * `helm lint` does not catch the chart's render-time `fail` -- Helm 4 reports it as INFO, so
#     only `helm template` aborts, which is why rendering *without* a release file must fail;
#   * a `dotnet test` without ContinuousIntegrationBuild=true silently rewrites the lock files
#     instead of failing on drift.
#
# This does not replace README section 13: the manual acceptance list needs a cluster, and CI
# does not have one either.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${REPO_ROOT}" || exit 2

CHART="deploy/helm/redetim"
K8S_VERSION="1.32.0"
KUBECONFORM_CACHE="${TMPDIR:-/tmp}/kubeconform"

chart_only=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --chart-only) chart_only=1; shift ;;
        -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

for tool in helm kubeconform; do
    command -v "${tool}" >/dev/null 2>&1 || {
        echo "${tool} not found on PATH. It is in the Nix dev shell: nix develop" >&2
        exit 2
    }
done
if [[ "${chart_only}" -eq 0 ]] && ! command -v dotnet >/dev/null 2>&1; then
    echo "dotnet not found on PATH. It is in the Nix dev shell: nix develop" >&2
    exit 2
fi

failed=0
step() { printf '\n==> %s\n' "$1"; }
fail() { echo "    FAILED: $1" >&2; failed=1; }

if [[ "${chart_only}" -eq 0 ]]; then
    step "Dependencies restore against their committed lock files"
    ./scripts/check-repro.sh || fail "check-repro.sh"

    step "Test suite in locked mode"
    dotnet test -p:ContinuousIntegrationBuild=true || fail "dotnet test"

    step "Lock files unchanged by the test run"
    git diff --exit-code -- '*packages.lock.json' || fail "a lock file was rewritten -- see the diff above"
else
    echo "==> skipping the .NET half (--chart-only)"
fi

# Git knows which release file was added last. On a fresh clone every file carries the same
# mtime, so `ls -t` would pick an arbitrary old one; locally built files are not committed yet,
# hence the fallback.
step "Select the release file"
REL="$(git log --diff-filter=A --name-only --format= -- 'deploy/releases/*.yaml' 2>/dev/null \
    | grep -v -- '-dirty\.' | grep . | head -1)"
[[ -n "${REL}" && -f "${REL}" ]] || REL="$(command ls -t deploy/releases/*.yaml 2>/dev/null | head -1)"
if [[ -z "${REL}" || ! -f "${REL}" ]]; then
    echo "    no release file under deploy/releases/ -- run ./scripts/build-images.sh first" >&2
    exit 1
fi
echo "    ${REL}"

# actions/cache does not create the directory on a miss and kubeconform refuses to start against
# a -cache path that does not exist. Same applies here.
mkdir -p "${KUBECONFORM_CACHE}"

validate() {
    local label="$1"; shift
    step "Chart: ${label}"
    helm lint "${CHART}" -f "${REL}" "$@" || fail "helm lint (${label})"
    helm template redetim "${CHART}" -n redetim -f "${REL}" "$@" \
        | kubeconform -strict -summary -kubernetes-version "${K8S_VERSION}" -cache "${KUBECONFORM_CACHE}" \
        || fail "helm template | kubeconform (${label})"
}

validate "without the HPA"
validate "with backend.autoscaling.enabled=true" --set backend.autoscaling.enabled=true

step "replicas belongs to the HPA or to the chart, never to both"
with_hpa="$(helm template redetim "${CHART}" -f "${REL}" --set backend.autoscaling.enabled=true \
    --show-only templates/backend.yaml 2>/dev/null | grep -c '^  replicas:')"
if [[ "${with_hpa}" != "0" ]]; then
    fail "backend.yaml renders 'replicas' while the HPA owns it"
elif helm template redetim "${CHART}" -f "${REL}" --show-only templates/backend.yaml 2>/dev/null \
    | grep -q '^  replicas: 2'; then
    echo "    ok"
else
    fail "backend.yaml does not render 'replicas: 2' without an HPA"
fi

step "Rendering without a release file must fail"
if helm template redetim "${CHART}" >/dev/null 2>&1; then
    fail "the chart rendered without a release file -- the tag guard is gone"
else
    echo "    ok"
fi

echo
if [[ "${failed}" -eq 0 ]]; then
    echo "==> gate passed. README section 13 still needs a cluster and a pair of eyes."
else
    echo "==> gate failed." >&2
fi
exit "${failed}"
