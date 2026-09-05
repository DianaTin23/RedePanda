#!/usr/bin/env bash
# Validates the Helm chart. Everything .github/workflows/chart.yml checks, in one place.
#
#   ./scripts/validate-chart.sh              # all four checks
#   ./scripts/validate-chart.sh --committed-only  # pick the release file from git only (CI)
#
# Four things are easy to leave out by hand, and each is the whole point of its check:
#   * the HPA variant has to be rendered *separately*, or nothing ever validates backend-hpa.yaml;
#   * `replicas` belongs to the HPA or to the chart, never both, or Helm and the autoscaler
#     overwrite each other and the pod count oscillates;
#   * rendering *without* a release file must fail, or `helm rollback` stops meaning anything;
#   * `helm lint` cannot be that last gate: Helm 4 reports a template `fail` as INFO and still
#     says "0 chart(s) failed". Only `helm template` actually aborts.
#
# Exit status: 0 when every check passed, 1 when one failed, 2 on a usage or tooling error.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
cd "$(repo_root)" || exit 2

CHART="deploy/helm/redetim"
K8S_VERSION="1.32.0"
# Overridable so CI can point it at the exact path actions/cache restores to; a mismatch there
# would not fail, it would just silently never hit the cache.
KUBECONFORM_CACHE="${KUBECONFORM_CACHE:-${TMPDIR:-/tmp}/kubeconform}"

select_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        --committed-only) select_args+=(--committed-only); shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

require_tool helm
require_tool kubeconform

REL="$(./scripts/select-release.sh ${select_args[@]+"${select_args[@]}"})" || exit 2
printf '==> release file: %s\n' "${REL}"

# The expected replica count comes from values.yaml -- not from a literal repeated here, and not
# from the rendered output, because comparing a render against itself passes no matter what the
# template does with the value. Same block-scoped read that build-images.sh uses for image names.
EXPECTED_REPLICAS="$(sed -n '/^backend:/,/^[a-zA-Z]/p' "${CHART}/values.yaml" \
    | sed -n 's/^[[:space:]]*replicas:[[:space:]]*\([0-9][0-9]*\).*$/\1/p' | head -1)"
if [[ -z "${EXPECTED_REPLICAS}" ]]; then
    echo "Could not read backend.replicas from ${CHART}/values.yaml" >&2
    exit 2
fi

failed=0
report() { echo "    FAIL: $*" >&2; failed=1; }

render() {
    local label="$1"; shift
    printf '==> chart renders %s\n' "${label}"
    if ! helm template redetim "${CHART}" -n redetim -f "${REL}" "$@" >/dev/null; then
        report "helm template failed ${label}"
        return
    fi
    helm lint "${CHART}" -f "${REL}" "$@" >/dev/null || report "helm lint failed ${label}"
    helm template redetim "${CHART}" -n redetim -f "${REL}" "$@" \
        | kubeconform -strict -summary -kubernetes-version "${K8S_VERSION}" \
            -cache "${KUBECONFORM_CACHE}" || report "kubeconform failed ${label}"
    echo "    ok"
}

mkdir -p "${KUBECONFORM_CACHE}"
render "without an HPA"
render "with backend.autoscaling.enabled=true" --set backend.autoscaling.enabled=true

echo "==> replicas belongs to the HPA or to the chart, never both"
with_hpa="$(helm template redetim "${CHART}" -f "${REL}" --set backend.autoscaling.enabled=true \
    --show-only templates/backend.yaml 2>/dev/null | grep -c '^  replicas:')"
without_hpa="$(helm template redetim "${CHART}" -f "${REL}" \
    --show-only templates/backend.yaml 2>/dev/null | sed -n 's/^  replicas: //p' | head -1)"
if [[ "${with_hpa}" != "0" ]]; then
    report "backend.yaml renders 'replicas' while the HPA owns it"
elif [[ -z "${without_hpa}" ]]; then
    report "backend.yaml renders no 'replicas' even though no HPA is active"
elif [[ "${without_hpa}" != "${EXPECTED_REPLICAS}" ]]; then
    report "backend.yaml renders 'replicas: ${without_hpa}', values.yaml says ${EXPECTED_REPLICAS}"
else
    echo "    ok (replicas: ${EXPECTED_REPLICAS} without an HPA, absent with one)"
fi

echo "==> rendering without a release file must fail"
if helm template redetim "${CHART}" >/dev/null 2>&1; then
    report "the chart rendered without a release file -- the tag guard is gone"
else
    echo "    ok"
fi

exit "${failed}"
