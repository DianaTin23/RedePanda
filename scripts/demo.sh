#!/usr/bin/env bash
# Opens the port-forwards for the demo and writes the release CA next to them.
#
#   ./scripts/demo.sh
#
# Frontend (8443 and the 8080 redirect), Prometheus (9090) and the collector (8889), plus the
# release CA so every curl below can verify against it instead of falling back to -k. The demo
# walkthrough that uses these ports is README section 8.
#
# NAMESPACE, RELEASE and CA_FILE override the defaults (redetim, redetim, $TMPDIR/<release>-ca.crt)
# for a release installed under another name.
#
# Runs until Ctrl+C; the trap closes every forward it opened.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

case "${1-}" in
    -h|--help) usage ;;
    "") ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
esac

NAMESPACE="${NAMESPACE:-redetim}"
RELEASE="${RELEASE:-redetim}"
CA_FILE="${CA_FILE:-${TMPDIR:-/tmp}/${RELEASE}-ca.crt}"

pids=()
cleanup() {
    echo
    echo "==> Closing port-forwards"
    for pid in "${pids[@]:-}"; do
        [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

forward() {
    local target="$1" ports="$2" label="$3" scheme="${4:-https}"
    kubectl -n "${NAMESPACE}" port-forward "${target}" "${ports}" >/dev/null 2>&1 &
    pids+=($!)
    printf '  %-12s %s\n' "${label}" "${scheme}://localhost:${ports%%:*}"
}

echo "==> Writing the release CA to ${CA_FILE}"
kubectl -n "${NAMESPACE}" get secret "${RELEASE}-ca" \
    -o jsonpath='{.data.ca\.crt}' | base64 -d > "${CA_FILE}"

echo "==> Port-forwards into namespace '${NAMESPACE}'"
forward "svc/${RELEASE}-frontend" 8443:8443 "frontend"

forward "svc/${RELEASE}-frontend" 8080:8080 "redirect" "http"

forward "svc/${RELEASE}-prometheus" 9090:9090 "prometheus"

forward "deploy/${RELEASE}-otel-collector" 8889:8889 "collector"

echo
echo "Two browser windows on https://localhost:8443, same room  -> both see the message."
echo "Two browser windows, different rooms                      -> no mixing."
echo "Prometheus query: redetim_messages_sent_total"
echo
echo "From the shell, verified against the release CA:"
echo "  curl --cacert ${CA_FILE} https://localhost:8443/healthz"
echo "  curl --cacert ${CA_FILE} https://localhost:8889/metrics | grep redetim_"
echo "  curl -sS -o /dev/null -w '%{http_code} %{redirect_url}\\n' http://localhost:8080/"
echo
echo "Press Ctrl+C to stop."
wait
