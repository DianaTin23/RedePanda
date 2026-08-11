# Build context is this directory.
#   podman build -t redepanda-frontend:dev src/RedePanda.Frontend

FROM caddy:2.11.4-alpine AS caddybase

# The published binary carries CAP_NET_BIND_SERVICE so it can bind :80. We listen on :8080 and
# drop all capabilities in the pod, so remove the file capability to keep the two consistent.
RUN setcap -r /usr/bin/caddy

# Fail the build, not the deployment, on a broken Caddyfile.
COPY Caddyfile /etc/caddy/Caddyfile
RUN caddy validate --adapter caddyfile --config /etc/caddy/Caddyfile

# Plain alpine rather than the caddy image: roughly half the size, and it drops the /config and
# /data volumes plus the EXPOSE 80/443/2019 that we neither use nor want.
FROM alpine:3.23

# mailcap supplies /etc/mime.types, without which the file server guesses content types.
RUN apk add --no-cache ca-certificates mailcap

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

EXPOSE 8080

CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
