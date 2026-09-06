#!/usr/bin/env bash
# Validates the Helm chart. Everything .github/workflows/chart.yml checks, in one place.
#
#   ./scripts/validate-chart.sh              # all four checks
#   ./scripts/validate-chart.sh --committed-only  # pick the release file from git only (CI)
#
# These are easy to leave out by hand, and each is the whole point of its check:
#   * the HPA variant has to be rendered *separately*, or nothing ever validates backend-hpa.yaml;
#   * `replicas` belongs to the HPA or to the chart, never both, or Helm and the autoscaler
#     overwrite each other and the pod count oscillates;
#   * the ingress variant has to be rendered *without* the ingress too, or the off switch is
#     never exercised and quietly stops working;
#   * three names couple the ingress to the rest of the chart, and none of them is checked by
#     rendering, linting or schema validation. They fail at runtime, in the browser, as a
#     certificate warning or a 404 -- so they are checked here;
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
# ServersTransport is a CRD and is not in the Kubernetes schema set. A second location rather
# than -ignore-missing-schemas: that flag is global and would wave through a misspelt `kind:`
# in our own objects just as happily.
CRD_SCHEMAS="https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json"
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

# `helm template` refuses to render at all while a declared dependency is missing from charts/.
# `build` rather than `update` is the point: update resolves the version afresh and rewrites
# Chart.lock, which would make the pin decorative. Helm 4 will not fetch from a repository it
# does not know, so the one the lock already names is registered first -- into a throwaway file,
# so running this script never edits the caller's helm configuration.
echo "==> resolving chart dependencies"
DEP_REPOS="${TMPDIR:-/tmp}/redetim-chart-repos.yaml"
DEP_REPO="$(sed -n 's/^[[:space:]]*repository:[[:space:]]*//p' "${CHART}/Chart.lock" | head -1)"
if [[ -z "${DEP_REPO}" ]]; then
    echo "No repository in ${CHART}/Chart.lock -- run: helm dependency update ${CHART}" >&2
    exit 2
fi
helm repo add chart-dep "${DEP_REPO}" --repository-config "${DEP_REPOS}" --force-update >/dev/null \
    || { echo "could not reach ${DEP_REPO}" >&2; exit 2; }
helm dependency build "${CHART}" --repository-config "${DEP_REPOS}" >/dev/null \
    || { echo "helm dependency build failed -- is Chart.lock in step with Chart.yaml?" >&2; exit 2; }

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
            -schema-location default -schema-location "${CRD_SCHEMAS}" \
            -cache "${KUBECONFORM_CACHE}" || report "kubeconform failed ${label}"
    echo "    ok"
}

mkdir -p "${KUBECONFORM_CACHE}"
render "without an HPA"
render "with backend.autoscaling.enabled=true" --set backend.autoscaling.enabled=true
render "with ingress.enabled=false" --set ingress.enabled=false

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

# Everything below renders with `-n redetim`. Without it .Release.Namespace is "default", and
# the serverstransport annotation -- which carries the namespace -- would be checked against a
# value that never occurs in practice.
show() { helm template redetim "${CHART}" -n redetim -f "${REL}" "$@" 2>/dev/null; }

INGRESS="$(show --show-only templates/ingress.yaml)"
OFF="$(show --set ingress.enabled=false)"

echo "==> ingress.enabled=false leaves no trace of the ingress"
off_hits="$(printf '%s' "${OFF}" | grep -cE 'kind: Ingress|kind: ServersTransport|ingress-tls' || true)"
if [[ "${off_hits}" != "0" ]]; then
    report "the chart rendered ${off_hits} ingress line(s) with ingress.enabled=false"
else
    echo "    ok"
fi

echo "==> the ingress certificate is one the chart actually issues"
# The single most expensive typo available here: a secretName nothing mints leaves Traefik
# serving its own self-signed default, which looks like a CA problem and is not one.
SECRET="$(printf '%s' "${INGRESS}" | sed -n 's/^ *secretName: //p' | head -1)"
if [[ -z "${SECRET}" ]]; then
    report "the ingress renders no tls.secretName"
elif ! show --show-only templates/tls.yaml | grep -q "^  name: ${SECRET}$"; then
    report "the ingress wants secret '${SECRET}', which tls.yaml does not issue"
else
    echo "    ok (${SECRET})"
fi

echo "==> the ServersTransport names the certificate the frontend actually serves"
# Drift here fails at runtime as an x509 hostname error. Nothing else in this script, in
# `helm lint` or in kubeconform looks at it.
SERVER_NAME="$(printf '%s' "${INGRESS}" | sed -n 's/^  serverName: //p' | head -1)"
if [[ "${SERVER_NAME}" != "redetim-frontend" ]]; then
    report "ServersTransport serverName is '${SERVER_NAME}', the frontend certificate says redetim-frontend"
else
    echo "    ok (${SERVER_NAME})"
fi

echo "==> the frontend service points at the ServersTransport the chart renders"
TRANSPORT_REF="$(show --show-only templates/frontend.yaml \
    | sed -n 's/^ *traefik.ingress.kubernetes.io\/service.serverstransport: //p' | tr -d '"' | head -1)"
TRANSPORT_NAME="$(printf '%s' "${INGRESS}" | sed -n '/kind: ServersTransport/,$p' | sed -n 's/^  name: //p' | head -1)"
if [[ "${TRANSPORT_REF}" != "redetim-${TRANSPORT_NAME}@kubernetescrd" ]]; then
    report "the service references '${TRANSPORT_REF}', the rendered object is redetim-${TRANSPORT_NAME}@kubernetescrd"
else
    echo "    ok (${TRANSPORT_REF})"
fi

echo "==> the frontend service asks Traefik for TLS to the backend"
if ! show --show-only templates/frontend.yaml | grep -q 'service.serversscheme: https'; then
    report "serversscheme is missing -- Traefik would speak plaintext to Caddy's TLS port"
else
    echo "    ok"
fi

echo "==> the host rule is the value from values.yaml"
EXPECTED_HOST="$(sed -n '/^ingress:/,/^[a-zA-Z]/p' "${CHART}/values.yaml" \
    | sed -n 's/^[[:space:]]*host:[[:space:]]*\(.*\)$/\1/p' | head -1)"
RENDERED_HOST="$(printf '%s' "${INGRESS}" | sed -n 's/^ *- host: //p' | tr -d '"' | head -1)"
if [[ "${RENDERED_HOST}" != "${EXPECTED_HOST}" ]]; then
    report "the ingress renders host '${RENDERED_HOST}', values.yaml says '${EXPECTED_HOST}'"
else
    # ponytail: compares the host rule, not the SAN in the leaf. Decoding a certificate out of
    # multi-document YAML without yq is worse than the bug it would catch; tls.yaml derives the
    # SAN from this same value.
    echo "    ok (${RENDERED_HOST})"
fi

echo "==> rendering without a release file must fail"
if helm template redetim "${CHART}" >/dev/null 2>&1; then
    report "the chart rendered without a release file -- the tag guard is gone"
else
    echo "    ok"
fi

exit "${failed}"
