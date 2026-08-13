#!/usr/bin/env bash
# Opens the port-forwards needed for the demo and stops them again on Ctrl+C.
#
#   ./scripts/demo.sh
#
# Frontend    https://localhost:8443   the chat itself
# Prometheus  https://localhost:9090   query the four metrics
# Collector   https://localhost:8889/metrics   proof that the metrics really pass through it
#
# Everything is TLS, and the certificates are signed by the CA this release minted for itself --
# no browser and no system trust store knows it. The script therefore writes that CA out to a
# file and prints it, so `curl --cacert` verifies properly instead of `curl -k` pretending to.
# In a browser the first visit shows a warning; accepting it once per port is enough.
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

# The CA of this release, so curl can verify rather than skip. It lives in a Secret the chart
# creates on the first install and reuses on every upgrade.
echo "==> Writing the release CA to ${CA_FILE}"
kubectl -n "${NAMESPACE}" get secret "${RELEASE}-ca" \
    -o jsonpath='{.data.ca\.crt}' | base64 -d > "${CA_FILE}"

echo "==> Port-forwards into namespace '${NAMESPACE}'"
forward "svc/${RELEASE}-frontend" 8443:8443 "frontend"

# Not needed by the demo. It is forwarded so that the redirect can be demonstrated -- and so an
# http:// address someone still has does something better than hang.
forward "svc/${RELEASE}-frontend" 8080:8080 "redirect" "http"

forward "svc/${RELEASE}-prometheus" 9090:9090 "prometheus"

# Deliberately against the deployment, not the service: the collector's Prometheus port is
# exposed on the service, but its health port 13133 is not, and `port-forward svc/...` resolves
# ports through the service definition.
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
