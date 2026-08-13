#!/usr/bin/env bash
# Mints a throwaway certificate authority and one broker certificate for the secured local broker
# in docker-compose.sasl.yml.
#
#   ./make-tls.sh          # generate tls/ if it is not already there
#   ./make-tls.sh --force  # regenerate it
#
# The material is written to ./tls/ and is gitignored. It is worth exactly nothing: it exists so
# that the TLS and SASL code paths can be exercised on a laptop, and it is regenerated whenever
# anyone wants it. Nothing here is a model for how to run a real broker.
#
# openssl comes from the pinned Redpanda image rather than from the host. That is one fewer thing
# to have installed, and it is the same image the broker itself runs -- the repository already
# takes this approach for `caddy adapt` in section 15 of the README.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLS_DIR="${HERE}/tls"

# Kept in step with docker-compose.yml and with redpanda.image in the chart's values.yaml.
# check-digests.sh compares those two to each other; this one follows them by hand.
IMAGE="docker.io/redpandadata/redpanda:v26.2.1@sha256:9a47c1f8d6736f98fa2616f6f0b715c051cb0bdac1a1176e38321bf45a5b572d"

FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
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

# Generated inside the container and streamed out as a tar, rather than written into a bind mount.
# A bind mount would need the container to run as the right uid, and there is no single right
# answer to that: under rootless docker and podman the host user already *is* container root, so
# passing --user breaks it, while under rootful docker omitting --user leaves root-owned files on
# the host. Piping a tar through stdout sidesteps the question entirely and behaves the same on
# all three.
"${ENGINE}" run --rm --entrypoint sh "${IMAGE}" -c '
set -eu
cd "$(mktemp -d)"

# The SANs are what the client actually checks. `localhost` and 127.0.0.1 cover a backend running
# on the host; redpanda-0 covers one container talking to another.
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
    -subj "/CN=RedePanda local dev CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout broker.key -out broker.csr \
    -subj "/CN=redpanda-0" 2>/dev/null

openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out broker.crt -days 825 -sha256 -extfile san.cnf -extensions ext 2>/dev/null

rm -f broker.csr san.cnf ca.srl

# World-readable on purpose: the broker process inside the compose container is not the user that
# generated these, and this material is throwaway by construction.
chmod 644 ca.crt ca.key broker.crt broker.key

tar -c ca.crt ca.key broker.crt broker.key
' > "${TLS_DIR}/bundle.tar"

tar -x -f "${TLS_DIR}/bundle.tar" -C "${TLS_DIR}"
rm -f "${TLS_DIR}/bundle.tar"

echo "==> Wrote ${TLS_DIR#"${HERE}/"}/{ca.crt,ca.key,broker.crt,broker.key}"
echo
echo "    Point a client at it with:"
echo "      REDPANDA_SSL_CA_LOCATION=${TLS_DIR}/ca.crt"
