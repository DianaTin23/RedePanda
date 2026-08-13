# Build context is this directory.
#   podman build -t redepanda-frontend:dev src/RedePanda.Frontend

# Digest-pinned, like every other external image here: the tag is for humans, the digest is
# what resolves. It is the manifest-list digest, so this still builds on arm64 as well as
# amd64. scripts/check-digests.sh reports when upstream has moved.
FROM caddy:2.11.4-alpine@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648 AS caddybase

# The published binary carries CAP_NET_BIND_SERVICE so it can bind :80 and :443. We listen on
# :8443 and :8080, both above 1024, and drop all capabilities in the pod, so remove the file
# capability to keep the two consistent.
RUN setcap -r /usr/bin/caddy

# Fail the build, not the deployment, on a broken Caddyfile.
#
# `caddy adapt`, not `caddy validate`, and that is not a downgrade for this config. validate
# *provisions* the configuration, which means it loads the TLS app and opens the certificate
# files — and those are mounted from a Secret at deploy time, so they do not exist during the
# build. It fails with "open /etc/redepanda/tls/tls.crt: no such file or directory" on a
# perfectly good Caddyfile. adapt runs the same Caddyfile parser and rejects the same syntax and
# directive errors without touching the filesystem, which is exactly the class of mistake a
# build can still catch.
COPY Caddyfile /etc/caddy/Caddyfile
RUN caddy adapt --adapter caddyfile --config /etc/caddy/Caddyfile > /dev/null

# Plain alpine rather than the caddy image: roughly half the size, and it drops the /config and
# /data volumes plus the EXPOSE 80/443/2019 that we neither use nor want.
FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

# Everything below is copied, never installed. An `apk add` would reach out to a live Alpine
# mirror at build time and resolve whatever version it is serving that day, which would leave
# this the one unpinned input in an otherwise digest-pinned image. The caddy stage above
# already installs exactly these two packages upstream, and it is pinned by digest, so taking
# the files from there makes the result reproducible without a package manager in the picture.
#
# mime.types is what stops the file server from guessing content types; without it, .css and
# .js are served as application/octet-stream and the browser refuses them.
COPY --from=caddybase /etc/mime.types /etc/mime.types

# The system trust store. The proxy upstream is verified against the release CA rather than
# against this bundle, so nothing here signs anything this container talks to today — but Go
# builds its root pool from these files, and an empty pool turns any future public endpoint into
# an unhelpful "x509: certificate signed by unknown authority".
COPY --from=caddybase /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=caddybase /usr/bin/caddy /usr/bin/caddy
COPY --from=caddybase /etc/caddy/Caddyfile /etc/caddy/Caddyfile
COPY wwwroot/ /usr/share/caddy/

USER 65532:65532

# Even with auto_https off, Caddy instantiates its TLS app at startup and sweeps its storage
# directory. Without these it resolves to $HOME/.local/share/caddy, which does not exist on a
# read-only root filesystem, and it logs a storage error on every start. The pod mounts an
# emptyDir at each of these paths; see the chart's frontend template.
ENV XDG_DATA_HOME=/data \
    XDG_CONFIG_HOME=/config

# 8443 serves the site over TLS; 8080 exists only to redirect to it. The certificate and key
# are NOT baked in: they are mounted at /etc/redepanda/tls from the chart's Secret, because a
# certificate is deployment configuration and an image that carried one would be a release that
# could only ever be deployed once (12-Factor III).
EXPOSE 8443 8080

CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
