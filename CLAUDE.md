# CLAUDE.md

Leitfaden für Claude Code (claude.ai/code) in diesem Repository.

Diese Datei ist ein **Index, keine Quelle**. Jede Aussage hier steht ausführlich woanders;
wo beides auseinanderläuft, gilt das Ziel des Verweises.

## Sprache

README.md, `docs/`, die Kommentare in `values.yaml`/`.csproj` und die Prosa unter `.claude/`
(`agents/*.md`, `skills/*/SKILL.md`) sind auf **Deutsch**; die `--help`-Köpfe der Skripte,
`flake.nix`, die Shell-Skripte unter `.claude/` und die Code-Kommentare in C# sind auf
**Englisch**. Beim Bearbeiten die Sprache der jeweiligen Datei beibehalten.

## Wo die Begründungen stehen

Der Code trägt bewusst fast keine Prosa. Jede nicht offensichtliche Entscheidung steht in
`docs/` — **vor** einer Änderung an einem dieser Bereiche das zugehörige Dokument lesen:

| Bereich | Dokument |
|---|---|
| Dienste, Schnitt, geteilte Typen (`Contracts`), manuelle Kopplungen, Logging | `docs/architecture.md` |
| Producer, Consumer, Offsets, GroupId, Shutdown, `KafkaSecurity` | `docs/kafka.md` |
| SSE, Verlaufspuffer, Backpressure, Heartbeats, Resume | `docs/streaming.md` |
| Caddyfile, `app.js`, Frontend-Image | `docs/frontend.md` |
| Helm-Chart, TLS, Probes, Jobs, HPA | `docs/deployment.md` |
| Zentrale Build-Konfiguration, Lockfiles, Digest-Pins, `build-images.sh` | `docs/build.md` |

README.md bleibt maßgeblich für alles Bedienbare (Befehle, Konfigurationstabelle,
Abnahmeliste, bekannte Einschränkungen). **Ihre Abschnittsnummern sind stabil und werden aus
Code und Doku heraus referenziert** — beim Umsortieren die Verweise mitziehen.

## Befehle

```bash
nix develop                    # Dev-Shell: .NET 10, rpk, kubectl, helm, kubeconform, skopeo,
                               #            jq (nur fuer .claude/)

dotnet build                   # TreatWarningsAsErrors=true, keine Ausnahmen im Repo
dotnet test                    # gesamte Suite (RedeTim.Backend.Tests, xunit v3)
dotnet test --filter "FullyQualifiedName~ChatHistoryTests.RoomsAreKeptApart"  # ein Test

./scripts/validate-chart.sh    # beide HPA-Varianten, replicas-Kopplung, Negativfall
./scripts/check-repro.sh       # alle vier Projekte im locked mode gegen ihre Lockfiles
./scripts/check-digests.sh     # Digest-Drift + Broker-Parität lokal/Cluster (braucht skopeo)
```

`validate-chart.sh` ist die einzige Stelle, an der die Chart-Regeln stehen; CI, `/abnahme` und
der Edit-Hook rufen dasselbe Skript. Wer eine Regel ändert, ändert sie dort — und nur dort.

Lokaler Lauf ohne Kubernetes und die Variante gegen einen TLS/SASL-Broker: README Abschnitt 5.
Images bauen und pushen: `./scripts/build-images.sh [--release] [--push]`, README
Abschnitt 6. Demo mit Port-Forwards: `./scripts/demo.sh`.

**CI** (`.github/workflows/`): ein Workflow je Sache, kein Sammelbecken. `dotnet.yml` und
`chart.yml` laufen bei Push auf `main` (ohne `deploy/releases/**`), bei jedem PR, per
`workflow_dispatch` und per `workflow_call`; `release.yml` **nur** per `workflow_dispatch` auf
`main` und hängt per `needs` an den beiden; `digests.yml` wöchentlich. Details: README
Abschnitt 12.

Einen Cluster hat CI nicht — die manuelle Abnahmeliste in README Abschnitt 12 bleibt manuell.

## Was `.claude/` automatisiert

Eingecheckt, gilt also für jeden, der das Repo auscheckt. Vollständig in README Abschnitt 15.

- **Zwei Hooks** (`.claude/settings.json`): `lockfile-guard.sh` meldet nach jedem `dotnet`-Aufruf
  still neu geschriebene `packages.lock.json`; `chart-guard.sh` ruft nach jeder Änderung unter
  `deploy/helm/` `validate-chart.sh --quick` auf (offline, ohne kubeconform). Beide melden sich
  per Exit-Code 2.
- **Zwei Skills**, nur auf Zuruf (`.claude/skills/`, `disable-model-invocation`): `/abnahme` (das
  lokale Gate) und `/release` (Vorbedingungen plus `workflow_dispatch`). Ein Verzeichnis
  `.claude/commands/` gibt es nicht.
- **Zwei Subagents** vor einem PR: `kafka-invariant-reviewer` prüft die Invarianten unten am
  Diff, `doc-sync-checker` die Doku-Kopplungen und Abschnittsnummern.

## Architektur

```
Browser ──HTTPS──▶ Caddy (Frontend) ──proxy /api──▶ Backend ──Kafka──▶ Redpanda
   ▲                :8443              HTTPS :8443    │                (StatefulSet)
   └───── SSE (/api/stream) ◀─────────────────────────┘
```

Vier Projekte: `RedeTim.Contracts` (geteiltes Wire-Format + `KafkaSecurity`),
`RedeTim.Backend` (ASP.NET Core Minimal API), `RedeTim.ChatClient` (Konsolenclient **und**
Admin-Prozess: `--ensure-topic`), `RedeTim.Frontend`
(nur Caddyfile + vier statische Dateien, kein Build-Tooling).

Eine Trennung trägt den Entwurf und ist vorführbar:

- **Das Frontend spricht kein Kafka.** Nur `/api/...`, kein npm/CDN/Webfonts.

Es gibt **keine Telemetrie**: kein OpenTelemetry-SDK, keinen Collector, kein Prometheus und
keinen `/metrics`-Endpunkt. Das ist eine bewusste Entrümpelung — nichts davon wieder einbauen,
ohne dass jemand danach fragt.

### Die tragenden Invarianten

Wer sie bricht, bekommt keinen Fehler, sondern ein System, das falsch läuft. Die Begründung
steht jeweils in `docs/`; hier steht nur, was gilt.

- **Eine Consumer-Group je Pod** (`redetim-backend-<POD_NAME>`) ⇒ Fan-out, nicht Lastausgleich.
  `POD_NAME` hat im Cluster keinen Default: `ResolvePodName` **wirft**, wenn
  `KUBERNETES_SERVICE_HOST` gesetzt und `POD_NAME` leer ist. → `docs/kafka.md#eine-consumer-group-je-pod`
- **Die SSE-`id` ist der Kafka-Offset**, also brokereigen — daher weder Sticky Sessions noch
  Backplane, und ein Reconnect auf einer anderen Replica setzt per `Last-Event-ID` lückenlos
  auf. Heartbeats tragen bewusst **keine** ID. → `docs/streaming.md#wiederaufnahme`
- **Der Raum ist der Kafka-Record-Key.** Alle Nachrichten eines Raums liegen damit auf einer
  Partition, und die Offsets je Stream steigen streng monoton. Offsets sind **pro Partition**
  eindeutig; genau deshalb trägt die Konstruktion auch bei `chat.partitions > 1`.
- **`WireFormat` ist die einzige Stelle mit `JsonSerializer`-Optionen.** Backend und
  Konsolenclient dürfen nicht eigenständig serialisieren; Chat- *und* Präsenz-Payload gehen
  durch dieselben Optionen. Einzige gewollte Ausnahme: `PresenceKey`, dessen JSON ein
  undurchsichtiger Record-*Key* ist und dessen Form von den Records im kompaktierten Topic
  festliegt.
- **`KafkaSecurity.ApplyTo` gilt für *jeden* Kafka-Client im Repo** (Producer, Consumer, Admin).
  Ein neuer Client ohne diesen Aufruf funktioniert gegen den Plaintext-Demo-Broker und
  scheitert still gegen jeden abgesicherten; genau so entstand der Readiness-Bug.
  `BrokerReadinessTests` prüft das pro Client. → `docs/kafka.md#abgesicherte-broker`
- **Es gibt kein `GET /api/history`.** Der Verlauf sind die ersten Frames von `/api/stream`.

### Release-Modell

Der Image-Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus `Chart.yaml` + kurzer
Commit (`0.1.0-g103b98b`), bei unsauberem Baum plus Inhalts-Hash. `build-images.sh` schreibt
dazu `deploy/releases/<version>.yaml`; das Chart hat **keinen Default-Tag** und bricht ohne
Release-Datei beim Rendern ab. Das ist es, was `helm rollback` wirksam macht. Helm ist der
einzige Installationsweg. → `docs/build.md#die-release-datei-ist-das-release`

### Konfiguration

Ausschließlich Env-Variablen unter schlichten Namen; `BackendOptions.FromEnvironment()` liest
sie **explizit**. Genau eine Ausnahme: `ASPNETCORE_Kestrel__Certificates__Default__*`
(Framework-Eigentum). Zugangsdaten stehen **nie** in der ConfigMap oder in `values.yaml`,
immer per `secretKeyRef` aus `redpanda.auth.existingSecret`. Vollständige Tabelle:
README Abschnitt 9.

## Fallen beim Bearbeiten

- **Lockfiles.** `dotnet build/test/run` schreiben `packages.lock.json` still neu. Eine
  absichtliche Versionsänderung gehört mit dem neuen Lockfile in den Commit; eine
  unbeabsichtigte gehört verworfen. `./scripts/check-repro.sh` ist die Probe.
  → `docs/build.md#lockfiles-und-wann-sie-tatsächlich-etwas-erzwingen`
- **`Directory.Build.props`: XML verbietet `--` im Kommentar.** MSBuild meldet dann ein leeres
  `TargetFramework` aus einer völlig anderen Datei.
- **NuGet-Versionen gehören ausschließlich in `Directory.Packages.props`**; eine `Version` in
  einer `.csproj` bricht den Restore absichtlich. `TargetFramework` wird in
  `Directory.Build.props` angehoben.
- **Die Runtime-Basis der .NET-Images muss glibc sein** (Debian). `-alpine` (musl) und
  `-chiseled` scheitern erst zur Laufzeit beim ersten `ConsumerBuilder.Build()`, weil
  `Confluent.Kafka` native librdkafka-Assets mitbringt.
- **`replicas` im Backend-Deployment** darf nur gerendert werden, wenn *kein* HPA aktiv ist.
  `validate-chart.sh` prüft beide Richtungen und vergleicht gegen `values.yaml`.
- **`helm lint` fängt kein `fail` im Template** — Helm 4 stuft es auf INFO herab. Nur
  `helm template` bricht wirklich ab. Deshalb prüft `validate-chart.sh` den Negativfall durch
  Rendern *ohne* Release-Datei.
- **Manuelle Kopplungen ohne Prüfung** (Tabelle in `docs/architecture.md#manuelle-kopplungen`):
  die Textlängengrenze in `app.js` hängt an `ChatMessage.DefaultMaxTextLength`;
  `RedeTim-kafka-docker/docker-compose.yml` und `redpanda.image` in `values.yaml` müssen
  dasselbe Broker-Image benennen (`check-digests.sh` prüft das).
- **`TreatWarningsAsErrors=true` ohne eine einzige Ausnahme** im Repo: kein `#pragma warning`,
  kein `[SuppressMessage]`, kein `NoWarn`.
- Der `on:`/`concurrency:`-Block in `dotnet.yml` und `chart.yml` ist zwanzig Zeilen doppelt.
  Bewusst: GitHub Actions kennt für diese Blöcke kein Include.
