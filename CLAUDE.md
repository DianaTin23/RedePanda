# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Sprache

README.md, `docs/` und die Kommentare in `values.yaml`/`.csproj` sind auf **Deutsch**; die
`--help`-Köpfe der Skripte, `flake.nix` und die Code-Kommentare in C# sind auf **Englisch**.
Beim Bearbeiten die Sprache der jeweiligen Datei beibehalten.

## Wo die Begründungen stehen

Der Code trägt bewusst fast keine Prosa. Jede nicht offensichtliche Entscheidung steht in
`docs/` — **vor** einer Änderung an einem dieser Bereiche das zugehörige Dokument lesen:

| Bereich | Dokument |
|---|---|
| Dienste, Schnitt, geteilte Typen (`Contracts`), manuelle Kopplungen | `docs/architecture.md` |
| Producer, Consumer, Offsets, GroupId, Shutdown, `KafkaSecurity` | `docs/kafka.md` |
| SSE, Verlaufspuffer, Backpressure, Heartbeats, Resume | `docs/streaming.md` |
| Caddyfile, `app.js`, Frontend-Image | `docs/frontend.md` |
| OTel-SDK, Collector, Prometheus, Metriknamen | `docs/observability.md` |
| Helm-Chart, TLS, Probes, Jobs, HPA | `docs/deployment.md` |
| Zentrale Build-Konfiguration, Lockfiles, Digest-Pins, `build-images.sh` | `docs/build.md` |

README.md bleibt maßgeblich für alles Bedienbare (Befehle, Konfigurationstabelle,
Abnahmeliste, bekannte Einschränkungen). **Ihre Abschnittsnummern sind stabil und werden aus
Code und Doku heraus referenziert** — beim Umsortieren die Verweise mitziehen.

## Befehle

```bash
nix develop                    # Dev-Shell: .NET 10, rpk, kubectl, helm, kubeconform, skopeo

dotnet build                   # TreatWarningsAsErrors=true, keine Ausnahmen im Repo
dotnet test                    # gesamte Suite (RedeTim.Backend.Tests, xunit v3)
dotnet test --filter "FullyQualifiedName~ChatHistoryTests"   # eine Klasse
dotnet test --filter "FullyQualifiedName~ChatHistoryTests.RoomsAreKeptApart"  # ein Test

./scripts/check-repro.sh       # alle vier Projekte im locked mode gegen ihre Lockfiles
./scripts/check-digests.sh     # Digest-Drift + Broker-Parität lokal/Cluster (braucht skopeo)
```

Chart-Validierung (die Release-Datei ist Pflicht, sonst bricht das Rendern absichtlich ab):

```bash
REL=$(command ls -t deploy/releases/*.yaml | head -1)
helm lint deploy/helm/redetim -f "$REL"
helm template redetim deploy/helm/redetim -n redetim -f "$REL" \
  | kubeconform -strict -summary -kubernetes-version 1.32.0
```

Die HPA-Kombination (`--set backend.autoscaling.enabled=true`) muss **zusätzlich** gerendert
werden, sonst validiert niemand `backend-hpa.yaml`. `helm lint` fängt ein `fail` im Template
nicht — Helm 4 stuft es auf INFO herab; nur `helm template` bricht wirklich ab.

Lokaler Lauf ohne Kubernetes (Broker im Container, alles andere per `dotnet run`) und die
Variante gegen einen TLS/SASL-Broker: README Abschnitt 5. Images bauen und in kind/minikube
laden: `./scripts/build-images.sh [--load kind] [--release]`, README Abschnitt 6. Demo mit
Port-Forwards: `./scripts/demo.sh`.

Es gibt **kein CI**. Nichts von alledem läuft von selbst.

## Architektur

```
Browser ──HTTPS──▶ Caddy (Frontend) ──proxy /api──▶ Backend ──Kafka──▶ Redpanda
   ▲                :8443              HTTPS :8443    │                (StatefulSet)
   └───── SSE (/api/stream) ◀──────────────────────── │
                                                      │ OTLP/gRPC über TLS :4317 (push)
                                                      ▼
                                            OTel-Collector ──HTTPS :8889──▶ Prometheus
```

Vier Projekte: `RedeTim.Contracts` (geteiltes Wire-Format + `KafkaSecurity`),
`RedeTim.Backend` (ASP.NET Core Minimal API), `RedeTim.ChatClient` (Konsolenclient **und**
Admin-Prozess: `--ensure-topic`, `--describe-topic`, `--print-config`), `RedeTim.Frontend`
(nur Caddyfile + vier statische Dateien, kein Build-Tooling).

Zwei Trennungen tragen den Entwurf und sind vorführbar:

- **Das Frontend spricht kein Kafka.** Nur `/api/...`, kein npm/CDN/Webfonts.
- **Das Backend spricht kein Prometheus.** Es pusht über OTLP und hat **keinen
  `/metrics`-Endpunkt** (`curl` → 404). Keinen einbauen.

### Die tragenden Invarianten

Diese Eigenschaften sind der Grund, warum das System skaliert. Wer sie bricht, bekommt keinen
Fehler, sondern ein System, das falsch läuft:

- **Eine Consumer-Group je Pod** (`redetim-backend-<POD_NAME>`) ⇒ Fan-out, nicht
  Lastausgleich. Eine geteilte Group ließe die Browser an allen anderen Pods in einem Raum
  sitzen, der sich nie aktualisiert. `POD_NAME` hat deshalb im Cluster keinen Default —
  `ResolvePodName` **wirft**, wenn `KUBERNETES_SERVICE_HOST` gesetzt und `POD_NAME` leer ist.
- **Die SSE-`id` ist der Kafka-Offset.** Er gehört dem Broker, nicht dem Pod — deshalb braucht
  es weder Sticky Sessions noch Backplane, und ein Reconnect auf einer anderen Replica setzt
  per `Last-Event-ID` lückenlos und ohne Dublette auf. Heartbeats tragen bewusst **keine** ID.
- **Der Raum ist der Kafka-Record-Key**, damit alle Nachrichten eines Raums auf einer Partition
  bleiben und die Offsets je Stream streng monoton steigen.
- **`ChatMessageSerializer` ist die einzige Stelle mit `JsonSerializer`-Optionen.** Backend und
  Konsolenclient dürfen nicht eigenständig serialisieren.
- **`KafkaSecurity.ApplyTo` gilt für *jeden* Kafka-Client im Repo** (Producer, Consumer, Admin
  — neun Stellen). Ein neuer Client ohne diesen Aufruf funktioniert gegen den Plaintext-Demo-
  Broker und scheitert still gegen jeden abgesicherten; genau so entstand der Readiness-Bug.
  `BrokerReadinessTests` prüft das pro Client.
- **Es gibt kein `GET /api/history`.** Der Verlauf sind die ersten Frames von `/api/stream`.
- **Metrik-Instrumentnamen**: punktgetrennt, klein, **ohne** `_total`, **ohne** Einheit — die
  Suffixe hängt der Prometheus-Exporter des Collectors an. Im Backend steht bewusst kein
  `ConfigureResource(...AddService(...))`; Identität kommt aus `OTEL_SERVICE_NAME` /
  `OTEL_RESOURCE_ATTRIBUTES`.

### Release-Modell

Der Image-Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus `Chart.yaml` + kurzer
Commit (`0.1.0-g103b98b`), bei unsauberem Baum plus Inhalts-Hash. `build-images.sh` schreibt
dazu `deploy/releases/<version>.yaml`; das Chart hat **keinen Default-Tag** und bricht ohne
Release-Datei beim Rendern ab. Das ist es, was `helm rollback` wirksam macht. Helm ist der
einzige Installationsweg — ein gerendertes `rendered.yaml` gehört nicht ins Repo (es mintete
bei jedem Lauf eine CA samt vier privaten Schlüsseln).

### Konfiguration

Ausschließlich Env-Variablen unter schlichten Namen; `BackendOptions.FromEnvironment()` liest
sie **explizit**, statt sich auf ASP.NETs `Section__Key`-Autobinding zu verlassen. Genau zwei
Ausnahmen: `OTEL_*` (vom SDK selbst gelesen) und `ASPNETCORE_Kestrel__Certificates__Default__*`
(Framework-Eigentum). Zugangsdaten stehen **nie** in der ConfigMap oder in `values.yaml`, immer
per `secretKeyRef` aus `redpanda.auth.existingSecret`. Vollständige Tabelle: README Abschnitt 9.

## Fallen beim Bearbeiten

- **Lockfiles.** `dotnet build/test/run` schreiben `packages.lock.json` still neu. Locked mode
  hängt an `ContinuousIntegrationBuild` und ist im Alltag aus. Eine absichtliche
  Versionsänderung gehört mit dem neu geschriebenen Lockfile in den Commit; eine unbeabsichtigte
  Änderung nach einem `dotnet test` gehört verworfen. `./scripts/check-repro.sh` ist die Probe.
- **`Directory.Build.props`: XML verbietet `--` im Kommentar.** MSBuild meldet dann ein leeres
  `TargetFramework` aus einer völlig anderen Datei.
- **Die `--help`-Köpfe der Skripte sind Code**, kein Kommentar: `build-images.sh`,
  `check-digests.sh` und `check-repro.sh` geben feste Zeilenbereiche per `sed -n '2,14p' "$0"`
  aus. Wer Zeilen im Kopf einfügt oder löscht, muss den Bereich mitziehen.
- **NuGet-Versionen gehören ausschließlich in `Directory.Packages.props`**; eine `Version` in
  einer `.csproj` bricht den Restore absichtlich. `TargetFramework` wird in
  `Directory.Build.props` angehoben.
- **Die Runtime-Basis der .NET-Images muss glibc sein** (Debian). `-alpine` (musl) und
  `-chiseled` scheitern erst zur Laufzeit beim ersten `ConsumerBuilder.Build()`, weil
  `Confluent.Kafka` native librdkafka-Assets mitbringt.
- **`replicas` im Backend-Deployment** darf nur gerendert werden, wenn *kein* HPA aktiv ist —
  sonst überschreiben sich Helm und Autoscaler gegenseitig.
- **Manuelle Kopplungen ohne Prüfung** (Tabelle in `docs/architecture.md#manuelle-kopplungen`):
  die Textlängengrenze in `app.js` hängt an `ChatMessage.DefaultMaxTextLength`;
  `RedeTim-kafka-docker/docker-compose.yml` und `redpanda.image` in `values.yaml` müssen
  dasselbe Broker-Image benennen (`check-digests.sh` prüft das).
- **`TreatWarningsAsErrors=true` ohne eine einzige Ausnahme** im Repo: kein `#pragma warning`,
  kein `[SuppressMessage]`, kein `NoWarn`.
