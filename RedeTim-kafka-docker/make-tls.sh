#!/usr/bin/env bash
# Mints a local CA and a broker certificate into ./tls/, for the TLS/SASL variant of the
# local broker (README section 5, "Gegen einen abgesicherten Broker").
#
#   ./make-tls.sh           # no-op if ./tls/broker.crt already exists
#   ./make-tls.sh --force   # regenerate, overwriting what is there
#
# Needs docker or podman: the certificates are produced by the broker image itself, so that
# they match the version the compose file runs.
#
# Exit status: 0 on success or when the material already exists, 2 on a usage or tooling error.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLS_DIR="${HERE}/tls"

# KEEP IN SYNC: docker-compose.yml und redpanda.image in values.yaml (scripts/check-digests.sh).
IMAGE="docker.io/redpandadata/redpanda:v26.2.1@sha256:9a47c1f8d6736f98fa2616f6f0b715c051cb0bdac1a1176e38321bf45a5b572d"

FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        -h|--help) sed -e '1d' -e '/^[^#]/,$d' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -f "${TLS_DIR}/broker.crt" && "${FORCE}" -eq 0 ]]; then
    echo "==> ${TLS_DIR#"${HERE}/"}/ already exists. Use --force to regenerate."
    exit 0
fi

if ! command -v docker >/dev/null 2>&1 && ! command -v podman >/dev/null 2>&1; then
    echo "Neither docker nor podman found on PATH." >&2
    exit 1
fi
ENGINE="$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)"

mkdir -p "${TLS_DIR}"

"${ENGINE}" run --rm --entrypoint sh "${IMAGE}" -c '
set -eu
cd "$(mktemp -d)"

cat > san.cnf <<EOF
[req]
distinguished_name = dn
[dn]
[ext]
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, DNS:redpanda-0, IP:127.0.0.1
EOF

openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -keyout ca.key -out ca.crt \
    -subj "/CN=RedeTim local dev CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout broker.key -out broker.csr \
    -subj "/CN=redpanda-0" 2>/dev/null

openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out broker.crt -days 825 -sha256 -extfile san.cnf -extensions ext 2>/dev/null

rm -f broker.csr san.cnf ca.srl

chmod 644 ca.crt ca.key broker.crt broker.key

tar -c ca.crt ca.key broker.crt broker.key
' > "${TLS_DIR}/bundle.tar"

tar -x -f "${TLS_DIR}/bundle.tar" -C "${TLS_DIR}"
rm -f "${TLS_DIR}/bundle.tar"

echo "==> Wrote ${TLS_DIR#"${HERE}/"}/{ca.crt,ca.key,broker.crt,broker.key}"
echo
echo "    Point a client at it with:"
echo "      REDPANDA_SSL_CA_LOCATION=${TLS_DIR}/ca.crt"
