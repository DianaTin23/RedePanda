#!/usr/bin/env bash
# Opens the port-forwards needed for the demo and stops them again on Ctrl+C.
#
#   ./scripts/demo.sh
#
# Frontend    http://localhost:8080   the chat itself
# Prometheus  http://localhost:9090   query the four metrics
# Collector   http://localhost:8889/metrics   proof that the metrics really pass through it
set -euo pipefail

NAMESPACE="${NAMESPACE:-redepanda}"
RELEASE="${RELEASE:-redepanda}"

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
    local target="$1" ports="$2" label="$3"
    kubectl -n "${NAMESPACE}" port-forward "${target}" "${ports}" >/dev/null 2>&1 &
    pids+=($!)
    printf '  %-12s %s\n' "${label}" "http://localhost:${ports%%:*}"
}

echo "==> Port-forwards into namespace '${NAMESPACE}'"
forward "svc/${RELEASE}-frontend" 8080:8080 "frontend"
forward "svc/${RELEASE}-prometheus" 9090:9090 "prometheus"

# Deliberately against the deployment, not the service: the collector's Prometheus port is
# exposed on the service, but its health port 13133 is not, and `port-forward svc/...` resolves
# ports through the service definition.
forward "deploy/${RELEASE}-otel-collector" 8889:8889 "collector"

echo
echo "Two browser windows on http://localhost:8080, same room  -> both see the message."
echo "Two browser windows, different rooms                     -> no mixing."
echo "Prometheus query: redepanda_messages_sent_total"
echo
echo "Press Ctrl+C to stop."
wait
