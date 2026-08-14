FROM caddy:2.11.4-alpine@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648 AS caddybase

RUN setcap -r /usr/bin/caddy

COPY Caddyfile /etc/caddy/Caddyfile
RUN caddy adapt --adapter caddyfile --config /etc/caddy/Caddyfile > /dev/null

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

COPY --from=caddybase /etc/mime.types /etc/mime.types

COPY --from=caddybase /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=caddybase /usr/bin/caddy /usr/bin/caddy
COPY --from=caddybase /etc/caddy/Caddyfile /etc/caddy/Caddyfile
COPY wwwroot/ /usr/share/caddy/

USER 65532:65532

ENV XDG_DATA_HOME=/data \
    XDG_CONFIG_HOME=/config

EXPOSE 8443 8080

CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
