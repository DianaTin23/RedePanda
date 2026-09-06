# CLAUDE.md

Guide for Claude Code (claude.ai/code) in this repository.

## Language

**README.md is German — everything else is English.** The frontend's user-facing strings
(`index.html`, the status and error texts in `app.js`) are German too, because they are product
text. Comments, code, scripts and this file are English. Keep a file's language when editing it.

README.md is the submission document: it is deliberately short — pitch, setup on kind, the
CNCF rationale, the twelve factors, tests. It carries no section numbers and nothing outside it
references it by section. Detail belongs in the file it describes (a comment in `values.yaml`,
a `--help` in a script), not in a longer README.

## Commands

```bash
nix develop                    # dev shell: .NET 10, rpk, kubectl, helm, kubeconform, skopeo

dotnet build                   # TreatWarningsAsErrors=true, no exceptions in the repo
dotnet test                    # whole suite (RedeTim.Backend.Tests, xunit v3)
dotnet test --filter "FullyQualifiedName~ChatHistoryTests.RoomsAreKeptApart"  # a single test

./scripts/validate-chart.sh    # HPA + ingress variants, replicas coupling, ingress wiring, negative case
./scripts/check-repro.sh       # all four projects in locked mode against their lockfiles
./scripts/check-digests.sh     # digest drift + broker parity local/cluster (needs skopeo)
```

`validate-chart.sh` is the only place the chart rules live; CI calls the same script. Change a
rule there — and only there.

Local run without Kubernetes: `RedeTim-kafka-docker/docker-compose.yml` for the broker, then
`dotnet run` per project; `make-tls.sh` next to it brings up the TLS/SASL listener on :19093.
Building and pushing images: `./scripts/build-images.sh [--release] [--push]`.

**CI** (`.github/workflows/`): one workflow per concern, no catch-all. `dotnet.yml` and
`chart.yml` run on push to `main` (excluding `deploy/releases/**`), on every PR, via
`workflow_dispatch` and via `workflow_call`; `release.yml` runs **only** via `workflow_dispatch`
on `main` and hangs off the other two through `needs`; `digests.yml` runs weekly.

CI has no cluster: anything that needs `helm install` stays a manual check.

## Architecture

```
Browser ──HTTPS──▶ Traefik ──▶ Caddy (Frontend) ──proxy /api──▶ Backend ──Kafka──▶ Redpanda
   ▲                 :8443      :8443               HTTPS :8443    │                (StatefulSet)
   └──────────────── SSE (/api/stream) ◀────────────────────────────┘
```

Four projects: `RedeTim.Contracts` (shared wire format + `KafkaSecurity`), `RedeTim.Backend`
(ASP.NET Core Minimal API), `RedeTim.ChatClient` (console client **and** admin process:
`--ensure-topic`), `RedeTim.Frontend` (Caddyfile plus static files, no build tooling).

One separation carries the design and is demonstrable: **the frontend speaks no Kafka.** Only
`/api/...`, no npm, no CDN, no web fonts.

There is **no telemetry**: no OpenTelemetry SDK, no collector, no Prometheus, no `/metrics`
endpoint. That is a deliberate cleanup — do not add any of it back unasked.

### Load-bearing invariants

Breaking one of these produces no error, just a system that runs wrong.

- **One consumer group per pod** (`redetim-backend-<POD_NAME>`) ⇒ fan-out, not load balancing.
  `POD_NAME` has no default in the cluster: `ResolvePodName` **throws** when
  `KUBERNETES_SERVICE_HOST` is set and `POD_NAME` is empty — a shared group would hand each
  message to exactly one replica.
- **The SSE `id` is the Kafka offset**, so it belongs to the broker — hence neither sticky
  sessions nor a backplane, and a reconnect against a different replica resumes without gaps via
  `Last-Event-ID`. Heartbeats deliberately carry **no** id, so they cannot overwrite it.
- **The room is the Kafka record key.** All messages of a room therefore live on one partition,
  and the offsets per stream increase strictly monotonically. Offsets are unique **per
  partition**; that is exactly why the construction still holds at `chat.partitions > 1`.
- **`WireFormat` is the only place with `JsonSerializer` options.** Backend and console client
  must not serialize on their own; chat *and* presence payloads go through the same options. The
  one intended exception is `PresenceKey`, whose JSON is an opaque record *key* whose shape is
  fixed by the records already in the compacted topic.
- **`KafkaSecurity.ApplyTo` applies to *every* Kafka client in the repo** (producer, consumer,
  admin). A new client without that call works against the plaintext demo broker and fails
  silently against every secured one — that is exactly how the readiness bug happened.
  `BrokerReadinessTests` checks this per client.
- **There is no `GET /api/history`.** The history is the first few frames of `/api/stream`.

### Release model

The image tag is **derived, not chosen**: `appVersion` from `Chart.yaml` plus the short commit
(`0.1.0-g103b98b`), plus a content hash when the tree is dirty. `build-images.sh` writes
`deploy/releases/<version>.yaml` for it; the chart has **no default tag** and aborts at render
time without a release file. That is what makes `helm rollback` meaningful. Helm is the only
installation path.

### Configuration

Environment variables only, under plain names; `BackendOptions.FromEnvironment()` reads them
**explicitly**. Exactly one exception: `ASPNETCORE_Kestrel__Certificates__Default__*` (owned by
the framework). Credentials **never** live in the ConfigMap or in `values.yaml`, always via
`secretKeyRef` from `redpanda.auth.existingSecret`. The switches are documented as comments in
`deploy/helm/redetim/values.yaml`.

## Editing traps

- **Lockfiles.** `dotnet build/test/run` silently rewrite `packages.lock.json`. An intentional
  version change belongs in the commit together with the new lockfile; an unintentional one
  belongs in the bin. `./scripts/check-repro.sh` is the probe.
- **`Directory.Build.props`: XML forbids a double hyphen inside a comment.** MSBuild then
  reports an empty `TargetFramework` coming from a completely different file.
- **NuGet versions belong exclusively in `Directory.Packages.props`**; a `Version` in a `.csproj`
  breaks the restore on purpose. `TargetFramework` is raised in `Directory.Build.props`.
- **The runtime base of the .NET images must be glibc** (Debian). `-alpine` (musl) and
  `-chiseled` only fail at runtime, on the first `ConsumerBuilder.Build()`, because
  `Confluent.Kafka` ships native librdkafka assets.
- **`replicas` in the backend deployment** may only be rendered when *no* HPA is active.
  `validate-chart.sh` checks both directions and compares against `values.yaml`.
- **`helm lint` does not catch a `fail` in a template** — Helm 4 downgrades it to INFO. Only
  `helm template` really aborts. That is why `validate-chart.sh` renders the negative case
  *without* a release file.
- **Manual couplings with no check**: the text length limit in `app.js` hangs off
  `ChatMessage.DefaultMaxTextLength`; `RedeTim-kafka-docker/docker-compose.yml` and
  `redpanda.image` in `values.yaml` must name the same broker image (`check-digests.sh` checks
  that one).
- **`TreatWarningsAsErrors=true` with not a single exception** in the repo: no
  `#pragma warning`, no `[SuppressMessage]`, no `NoWarn`.
- The `on:`/`concurrency:` block in `dotnet.yml` and `chart.yml` is twenty duplicated lines. That
  is deliberate: GitHub Actions has no include for those blocks.
