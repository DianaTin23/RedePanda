#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-redepanda}"
RELEASE="${RELEASE:-redepanda}"
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
echo "Prometheus query: redepanda_messages_sent_total"
echo
echo "From the shell, verified against the release CA:"
echo "  curl --cacert ${CA_FILE} https://localhost:8443/healthz"
echo "  curl --cacert ${CA_FILE} https://localhost:8889/metrics | grep redepanda_"
echo "  curl -sS -o /dev/null -w '%{http_code} %{redirect_url}\\n' http://localhost:8080/"
echo
echo "Press Ctrl+C to stop."
wait
