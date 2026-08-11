# RedePanda — Umbauplan bis zur Abgabe (07.09., 24:00)

Arbeitsdokument für die Gruppe. Abgehakt wird direkt hier per Commit.

---

## 0. Zielbild in einem Satz

Ein Browser-Chat, bei dem **Frontend (Caddy)** und **Backend (ASP.NET Core)** als zwei
getrennte, selbst geschriebene Deployments in Kubernetes laufen und über
**Redpanda** (Kafka-Protokoll) miteinander sprechen — installiert per **Helm**,
instrumentiert per **OpenTelemetry** (OTLP-Push an einen **OpenTelemetry Collector**)
und ausgewertet in **Prometheus**.

```text
Browser ──HTTP──▶ Caddy (Frontend-Pod) ──proxy /api──▶ Backend-Pod ──Kafka──▶ Redpanda
   ▲                                                      │  │
   └────────── SSE-Stream (/api/stream) ◀─────────────────┘  │
                                                             │ OTLP/gRPC :4317 (push)
                                                             ▼
                                                    OTel-Collector-Pod
                                                             │ :8889 /metrics
                                                             ▼ (scrape)
                                                      Prometheus-Pod
```

Der Metrikpfad zeigt bewusst **vom Backend weg**: die Anwendung pusht, sie wird nicht
gescrapt. Sie kennt Prometheus nicht — nur einen OTLP-Endpunkt aus einer Env-Variable.

---

## 1. Getroffene Entscheidungen (und warum)

Diese Punkte weichen bewusst vom ursprünglichen Entwurf ab. Wer anderer Meinung
ist: hier diskutieren, nicht während der Implementierung umschwenken.

| # | Entscheidung | Begründung |
|---|---|---|
| E1 | **SSE statt SignalR** für Live-Nachrichten | SignalR braucht WebSocket-Upgrade durch den Proxy, Sticky Sessions und bei >1 Replica ein Backplane. SSE ist ~40 Zeilen Backend, braucht keine Client-Library und funktioniert durch jeden Proxy. Für einen reinen Server→Browser-Broadcast reicht es exakt. |
| E2 | **Redpanda als eigenes StatefulSet im Chart**, nicht das offizielle Chart | Wir haben in `RedePanda-kafka-docker/docker-compose.yml` bereits eine funktionierende Flag-Kombination (`--mode=dev-container --smp=1`). Die übernehmen wir 1:1. Das offizielle Chart ist auf Produktionscluster ausgelegt und zieht mehr Ressourcen und Konfiguration nach, als wir demonstrieren wollen. |
| E3 | **Konsolenclient bleibt erhalten** | Ist bestehende Arbeit und belegt in der Demo schön, dass Backend und Client wirklich dasselbe Kafka-Topic teilen. Er wird nur auf Env-Konfiguration umgestellt, sonst nicht angefasst. |
| E4 | **Ein Topic, Raum als Feld + Kafka-Key** | Einfacher zu deployen (ein Init-Job) und die Raumtrennung filtert das Backend serverseitig. Key = `Room` sichert die Reihenfolge pro Raum, falls das Topic je mehr Partitionen bekommt. |
| E5 | **Backend läuft mit 1 Replica**, Skalierung nur dokumentiert | Mehrere Replicas funktionieren mit unserem Consumer-Design (siehe E6) tatsächlich, aber ein Browser hängt immer nur an einem Pod. Wir zeigen es als bewusste Grenze im README statt es halb zu bauen. |
| E6 | **Consumer-GroupId bleibt pro Pod eindeutig** | `Consumer.cs:23` macht das heute schon richtig (`"kchat-" + Guid`). Beim Umbau **nicht** auf eine feste Group-ID vereinheitlichen — sonst bekäme bei mehreren Replicas jede Nachricht nur ein Pod und die anderen Browser sehen nichts. Im Backend nehmen wir statt des GUID den Pod-Namen (deterministisch, besser zu debuggen). |
| E7 | **Prometheus minimal selbst deployen**, nicht kube-prometheus-stack | Ein Deployment + ConfigMap mit `static_configs`; einziges Scrape-Ziel ist `redepanda-otel-collector:8889`. Der Stack würde 20 Minuten Cluster-Ressourcen fressen für Features (Operator, ServiceMonitor-CRDs, Alertmanager), die wir nicht zeigen. Der Collector macht ServiceMonitors ohnehin überflüssig, weil er die einzige Scrape-Quelle ist. |
| E9 | **OpenTelemetry-SDK + Collector statt `prometheus-net`** | Das Backend kennt Prometheus nicht mehr, sondern nur einen OTLP-Endpunkt aus einer Env-Variable — das ist 12-Factor „Backing Services" im Reinformat, dieselbe Argumentation wie bei Redpanda. Der Collector ist der austauschbare Teil: das Monitoring-Backend lässt sich ohne Backend-Rebuild wechseln. Zusätzlich ist OpenTelemetry seit 11.05.2026 CNCF *graduated*, also eine weitere bewertbare CNCF-Technologie statt einer Community-Library. Preis: ein Pod und ein Hop mehr, ca. +4–6 h Aufwand. |
| E10 | **Nur Metriken — keine Traces, keine Logs über OTLP** | Der Collector könnte alle drei Signale, wir schalten bewusst nur die Metrics-Pipeline ein. Traces bräuchten Kontext-Propagierung von Hand über die Kafka-Grenze (`Confluent.Kafka` hat keine Auto-Instrumentierung) plus ein zweites Backend (Jaeger/Tempo) — nicht vor dem 07.09. Logs bleiben auf stdout, siehe 12-Factor. |
| E8 | **Manifeste = Helm-Templates**, zusätzlich ein gerendertes `deploy/k8s/rendered.yaml` | Die Aufgabe verlangt Manifeste; Templates *sind* Manifeste. Das gerenderte File erlaubt trotzdem `kubectl apply -f` ohne Helm und dient als Beleg. Kein doppelter Pflegeaufwand, weil generiert. |

---

## 2. Zielstruktur des Repos

```text
RedePanda/
├── README.md                      # Hauptdokumentation (Abgabe-relevant)
├── PLAN.md                        # dieses Dokument
├── RedePanda.sln
├── src/
│   ├── RedePanda.Contracts/       # ChatMessage + Validierung (shared)
│   ├── RedePanda.Backend/         # ASP.NET Core Web API
│   ├── RedePanda.Frontend/        # index.html, app.js, style.css, Caddyfile
│   └── RedePanda.ChatClient/      # bisheriger Konsolenclient (umgezogen)
├── tests/
│   └── RedePanda.Backend.Tests/   # xUnit
├── deploy/
│   ├── helm/redepanda/
│   │   ├── Chart.yaml
│   │   ├── values.yaml
│   │   └── templates/
│   │       ├── _helpers.tpl
│   │       ├── configmap.yaml
│   │       ├── backend.yaml       # Deployment + Service
│   │       ├── frontend.yaml      # Deployment + Service
│   │       ├── redpanda.yaml      # StatefulSet + Service
│   │       ├── topic-job.yaml     # Helm post-install hook
│   │       ├── otel-collector.yaml # Deployment + Service + Collector-Config-ConfigMap
│   │       └── prometheus.yaml    # Deployment + Service + Scrape-Config (Ziel: Collector)
│   └── k8s/rendered.yaml          # generiert via `helm template`
├── scripts/
│   ├── build-images.sh / .ps1     # Build + in den lokalen Cluster laden
│   └── demo.sh / .ps1             # Port-Forwards für die Vorführung
└── RedePanda-kafka-docker/        # bleibt: Redpanda für lokale Entwicklung ohne k8s
```

---

## 3. Phasen

Reihenfolge ist bewusst **Infrastruktur zuerst**. Die Kubernetes- und
Redpanda-Schicht ist der Teil, der unerwartet Zeit frisst; die Chat-Features sind
der planbare Teil. Wenn wir uns verschätzen, wollen wir das in Woche 1 merken.

### Phase 0 — Repo-Umbau (11.–13.08.)

- [ ] Verzeichnisstruktur nach Abschnitt 2 anlegen, `git mv` für den bestehenden Client
- [ ] `RedePanda.sln` auf die neuen Pfade aktualisieren
- [ ] `RedePanda.Contracts` mit dem neuen Modell:
      ```csharp
      public record ChatMessage(string Room, string Nickname, string Text, DateTimeOffset Timestamp);
      ```
      plus eine statische `Validate(...)`-Methode (nicht leer, Trim, Längenlimits) —
      die wird von Backend **und** Tests benutzt
- [ ] Konsolenclient: `ConfigureBootstrap()` und `Bootstrap.cs` ersatzlos löschen,
      stattdessen `REDPANDA_BOOTSTRAP_SERVERS` / `REDPANDA_TOPIC` aus der Umgebung
- [ ] `local.json`, `lan.json`, `temp-lan.json`, `temp-env.lan` entfernen; `.gitignore` aufräumen
- [ ] `.dockerignore` für Backend und Frontend

**Fertig, wenn:** `dotnet build` grün ist und der Konsolenclient mit
`REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 dotnet run --project src/RedePanda.ChatClient`
gegen das Compose-Redpanda chattet.

### Phase 1 — Walking Skeleton im Cluster (14.–17.08.) ⬅ kritischste Phase

Ziel: **alles deployed, nichts kann Chat.** Erst wenn das steht, kommt Logik dazu.

- [ ] Backend-Projekt mit *nur* `GET /health/live` → `200 OK`
- [ ] Frontend: statische `index.html` mit „RedePanda" + `nginx.conf` mit Proxy auf `/api`
- [ ] Zwei Dockerfiles (Multi-Stage, `USER` non-root, Backend `:8080`, Frontend `:8080` via nginx-unprivileged)
- [ ] `scripts/build-images.sh` — baut beide Images und lädt sie in den lokalen Cluster
- [ ] Helm-Chart mit Backend, Frontend, Redpanda-StatefulSet, ConfigMap, Topic-Job,
      **OTel-Collector** (Pipeline schon verdrahtet, aber noch schickt niemand etwas hin)
- [ ] `helm upgrade --install redepanda ./deploy/helm/redepanda -n redepanda --create-namespace`
- [ ] Port-Forward → Seite lädt, `/api/health/live` antwortet durch den nginx-Proxy

**Fertig, wenn:** alle Pods `Running`/`Ready`, der Topic-Job `Completed`, und
`kubectl exec` in den Backend-Pod erreicht `redpanda:9092` **sowie
`redepanda-otel-collector:4317`**.

> **Warum der Collector schon hier und nicht erst in Phase 3:** Was am Collector Zeit
> frisst, sind Deployment-Themen — `readOnlyRootFilesystem`, non-root-UID, Config-Mount,
> Probe-Port 13133 —, nicht Code. Genau die Sorte Überraschung, die wir laut Präambel
> in Woche 1 haben wollen und nicht drei Tage vor Abgabe. In Phase 3 bleibt dann nur
> noch die SDK-Seite plus Prometheus.

### Phase 2 — Chat-Funktionalität (18.–23.08.)

- [ ] `ChatProducer` (Singleton) — Produce mit `Key = Room`
- [ ] `ChatConsumerService : BackgroundService` — GroupId aus `POD_NAME`, `AutoOffsetReset.Latest`
- [ ] `ChatBroadcaster` — In-Memory-Fan-out an alle offenen SSE-Verbindungen, Raumfilter
- [ ] `POST /api/messages` — validiert, setzt `Timestamp` serverseitig, produziert
- [ ] `GET /api/stream?room=X` — `text/event-stream`, Heartbeat alle 15s
- [ ] `GET /health/ready` — prüft Broker-Metadaten (Ergebnis ~5s cachen)
- [ ] Frontend: Nickname/Raum-Eingabe, Nachrichtenliste, `EventSource`, Reconnect-Anzeige

**Fertig, wenn:** zwei Browserfenster im selben Raum sich sehen, in verschiedenen
Räumen nicht — und der Konsolenclient dieselben Nachrichten mitliest.

### Phase 3 — Observability: OTel-SDK → Collector → Prometheus (24.–28.08.)

**SDK-Seite (Strang A):**

- [ ] NuGet (alle stabil, Stand 08/2026): `OpenTelemetry.Extensions.Hosting` 1.17.0,
      `OpenTelemetry.Exporter.OpenTelemetryProtocol` 1.17.0,
      `OpenTelemetry.Instrumentation.AspNetCore` 1.17.0,
      optional `OpenTelemetry.Instrumentation.Runtime` 1.17.0.
      **Finger weg von** `OpenTelemetry.Instrumentation.Process` (nur `-rc`),
      `OpenTelemetry.Instrumentation.ConfluentKafka` (nur `0.2.0-alpha`) und
      `OpenTelemetry.Exporter.Prometheus.AspNetCore` (nur `-beta`) — alles Prerelease.
- [ ] Wiring in `Program.cs` (`using OpenTelemetry;` **nicht vergessen** — `ConfigureResource`
      und `WithMetrics` liegen im Wurzel-Namespace, sonst CS1061):
      ```csharp
      builder.Services.AddOpenTelemetry()
          .ConfigureResource(r => r.AddService("redepanda-backend"))
          .WithMetrics(m => m
              .AddAspNetCoreInstrumentation()
              .AddMeter("RedePanda")
              .AddOtlpExporter());
      ```
- [ ] **Kein `/metrics`-Endpunkt im Backend mehr** — kein `MapMetrics()`, kein Scrape-Port,
      keine Prometheus-Annotationen am Backend-Service. Das Backend pusht ausschließlich.
      Muss explizit dastehen, sonst baut es jemand „sicherheitshalber" trotzdem ein.
- [ ] Vier fachliche Instrumente über `System.Diagnostics.Metrics.Meter("RedePanda")`,
      **mit Punkten, ohne `_total`, ohne Unit** (Herleitung siehe 4.7):

| Instrument in C# | Typ | Name in Prometheus |
|---|---|---|
| `redepanda.messages.sent` | `Counter<long>` | `redepanda_messages_sent_total` |
| `redepanda.messages.received` | `Counter<long>` | `redepanda_messages_received_total` |
| `redepanda.kafka.errors` | `Counter<long>` | `redepanda_kafka_errors_total` |
| `redepanda.active_connections` | `ObservableGauge<int>` | `redepanda_active_connections` |

- [ ] `redepanda.active_connections` als **ObservableGauge** implementieren, dessen Callback
      die `.Count`-Property der SSE-Subscriber-Collection im `ChatBroadcaster` liest — nicht
      als `UpDownCounter`. Ein Zähler, den man selbst hoch- und runterzählt, driftet bei jedem
      Client-Abbruch, der am `finally` vorbeigeht, und heilt nie wieder. Der Callback liest
      immer den echten Stand und liefert ab Prozessstart eine 0 statt „No data".
      ⚠ Das ist eine echte Codeänderung am `ChatBroadcaster` aus Phase 2 (kein `.Inc()`/`.Dec()`).

**Cluster-Seite (Strang B):**

- [ ] OTel-Collector scharfschalten: Ports 4317 (OTLP/gRPC), 8889 (Prometheus-Exporter),
      8888 (Collector-Eigenmetriken), 13133 (`health_check`). Per `values.yaml` abschaltbar.
- [ ] Prometheus-Deployment + Scrape-Config im Chart, per `values.yaml` abschaltbar.
      Einziger Diff zum ursprünglichen Plan: Ziel ist `redepanda-otel-collector:8889`
      statt `redepanda-backend:8080`. **`honor_labels: true` ist Pflicht** (siehe 4.7).
      `values.yaml` bekommt damit zwei Toggles statt einem — und Prometheus ohne Collector
      ergibt keinen Sinn mehr, die Abhängigkeit gehört dokumentiert.
- [ ] Screenshot fürs README: Zähler steigt während der Demo sichtbar an — **plus ein
      zweiter Beleg, dass der Weg wirklich über den Collector geht** (Prometheus-Targets-Seite
      mit `redepanda-otel-collector:8889` = `UP`, oder `curl` auf `:8889/metrics`).
      Ohne den zweiten Beleg ist auf dem Bild nicht unterscheidbar, ob nicht doch direkt
      gescrapt wird — und genau der Unterschied ist der Mehrwert des Umbaus.

### Phase 4 — Härtung & Tests (29.08.–02.09.)

- [ ] Graceful Shutdown: SIGTERM → Consumer `Close()`, Producer `Flush()`,
      `terminationGracePeriodSeconds` gesetzt
- [ ] `resources.requests/limits` und `securityContext` (`runAsNonRoot`, `readOnlyRootFilesystem`)
      für alle Pods — „alle" heißt jetzt zwei Pods mehr:
      - **Collector:** Image `otel/opentelemetry-collector:0.158.0` läuft bereits als UID 10001
        und kommt mit `readOnlyRootFilesystem` ohne jeden Schreib-Mount aus (verifiziert)
      - **Prometheus:** braucht bei `readOnlyRootFilesystem` ein `emptyDir` auf `/prometheus`
        für die TSDB. Und: wer `args:` setzt, ersetzt damit das komplette `CMD` — `--config.file`
        und `--storage.tsdb.path` müssen dann beide wieder mit rein, sonst sucht Prometheus
        `./prometheus.yml` im leeren WORKDIR und beendet sich mit Config-Ladefehler
- [ ] Liveness/Readiness-Probes für **alle** Pods: Frontend, Backend, Redpanda,
      **Collector (`:13133/`, dafür muss `health_check` unter `extensions:` UND unter
      `service.extensions:` stehen — sonst ist der Port tot) und Prometheus
      (`/-/healthy` bzw. `/-/ready` auf `:9090`)**
- [ ] Strukturiertes Logging nach stdout, Level über `LOG_LEVEL`
- [ ] xUnit-Tests (bewusst wenige, dafür sinnvolle):
      1. gültige Nachricht wird akzeptiert
      2. leerer Nickname / leerer Text / leerer Raum → abgelehnt
      3. Text über `MAX_MESSAGE_LENGTH` → abgelehnt
      4. JSON-Roundtrip `ChatMessage` → String → `ChatMessage`
      5. `Timestamp` vom Client wird ignoriert und serverseitig gesetzt

Alles Weitere (zwei Browserfenster, Pod-Neustart, Helm-Deinstallation) ist eine
**manuelle Abnahme-Checkliste im README**, keine Testautomatisierung. Kein CI verlangt.

### Phase 5 — Dokumentation & Probelauf (03.–06.09.)

- [ ] README nach Abschnitt 5 dieses Plans
- [ ] `helm template` → `deploy/k8s/rendered.yaml` committen
- [ ] **Kompletter Probelauf auf einem frischen Cluster**, strikt nach eigener README —
      jemand, der es nicht gebaut hat, führt ihn durch
- [ ] Repository auf public stellen, Gruppenmitglieder eintragen

**07.09.:** Puffer. Nichts Neues mehr anfangen.

---

## 4. Technische Details, die erfahrungsgemäß schiefgehen

Diese sieben Punkte kosten sonst je einen halben Tag Suche.

**4.1 nginx puffert SSE.** Ohne das hier kommen Nachrichten erst verzögert oder gar nicht an:
```nginx
location /api/stream {
    proxy_pass http://redepanda-backend:8080;
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 3600s;
    proxy_http_version 1.1;
}
```

**4.2 Images landen nicht im Cluster.** Lokal gebaute Images kennt der Cluster nicht →
`ImagePullBackOff`. Deshalb `imagePullPolicy: IfNotPresent` **und** explizit laden:
```bash
kind load docker-image redepanda-backend:dev redepanda-frontend:dev      # kind
minikube image load redepanda-backend:dev redepanda-frontend:dev         # minikube
```
Docker Desktop mit aktiviertem Kubernetes braucht das nicht. Alle drei Wege ins README.

**4.3 Der Topic-Job startet vor Redpanda.** Ohne Retry: `CrashLoopBackOff`.
Als Helm-Hook mit Warteschleife:
```yaml
annotations:
  "helm.sh/hook": post-install,post-upgrade
  "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 10
```
Im Container: `until rpk cluster health --brokers redpanda:9092 | grep -q "Healthy:.*true"; do sleep 3; done`
danach `rpk topic create "$REDPANDA_TOPIC" -p 1 -r 1 --brokers redpanda:9092`.

**4.4 Redpanda-Flags in Kubernetes.** Ohne die Seastar-Begrenzungen belegt der Pod die
halbe Node oder startet nicht. Wir übernehmen die Compose-Flags und ergänzen Speicher:
```yaml
command: [redpanda, start, --mode=dev-container, --smp=1, --memory=1G,
          --reserve-memory=0M, --overprovisioned,
          --kafka-addr=internal://0.0.0.0:9092,
          --advertise-kafka-addr=internal://redpanda:9092]
```
Speicher via `volumeClaimTemplates` im StatefulSet — **kein separat deklariertes PVC**.
Image mit fester Version pinnen, nicht `:latest` wie aktuell in der Compose-Datei.

**4.5 Env-Variablen und ASP.NET.** ASP.NET erwartet standardmäßig `Section__Key`.
Wir wollen aber die schlichten Namen aus der Aufgabenstellung. Also in `Program.cs`
explizit lesen statt auf Auto-Binding zu hoffen — dann stimmt auch die README-Tabelle.

**Ausnahme sind die `OTEL_*`-Variablen.** Die liest das OpenTelemetry-SDK selbst, sie sind
in der OTel-Spezifikation genormt und dürfen deshalb **nicht** in `Program.cs` nachgebaut
werden. Für die 12-Factor-Argumentation ist das sogar der bessere Fall: standardisierte
Config statt selbst erfundener Namen. Wer sie „der Konsistenz halber" manuell ausliest und
dem SDK per Code übergibt, erzeugt zwei Wahrheiten.
Kleiner Fallstrick dabei: mit `AddOtlpExporter()` greifen **ausschließlich** die generischen
`OTEL_EXPORTER_OTLP_*`-Variablen. Die signalspezifischen (`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
usw.) werden still ignoriert — die gäbe es nur mit `UseOtlpExporter()`.

| Variable | Default | Verwendet von |
|---|---|---|
| `REDPANDA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Backend, Konsolenclient |
| `REDPANDA_TOPIC` | `redepanda-chat` | Backend, Konsolenclient, Topic-Job |
| `MAX_MESSAGE_LENGTH` | `500` | Backend |
| `ASPNETCORE_URLS` | `http://+:8080` | Backend |
| `POD_NAME` | (fieldRef) | Backend (Consumer-GroupId) |
| `LOG_LEVEL` | `Information` | Backend |
| `BACKEND_HOST` | `redepanda-backend:8080` | Frontend (Caddyfile) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://redepanda-otel-collector:4317` | Backend |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Backend |
| `OTEL_SERVICE_NAME` | `redepanda-backend` | Backend (→ Prometheus-Label `job`) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `5000` (ms) | Backend |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.instance.id=$(POD_NAME)` | Backend (→ Label `instance`) |
| `OTEL_SDK_DISABLED` | `false` | Backend (Not-Aus für eine Demo ohne Collector) |

⚠ **`service.namespace` bewusst NICHT setzen.** Der Prometheus-Exporter des Collectors baut
das `job`-Label als `<service.namespace>/<service.name>` — mit gesetztem Namespace hieße das
Label plötzlich `redepanda/redepanda-backend` und jede PromQL-Abfrage aus dem README liefert
leer. Der Namespace steckt ohnehin schon im Pod-Namen.

**4.6 Der OTLP-Pfad ist im Demo-Zeitraum unsichtbar.** Das .NET-SDK exportiert Metriken
periodisch, Default **60 Sekunden**. Bei einer Fünf-Minuten-Vorführung sieht der Zähler
eingefroren aus und man debuggt eine halbe Stunde lang eine funktionierende Pipeline.

```yaml
OTEL_METRIC_EXPORT_INTERVAL: "5000"     # Backend, statt 60000
# und in der prometheus.yml:
scrape_interval: 5s                      # für den Collector-Job, statt 15s
```

Weiter: `4317` = gRPC, `4318` = HTTP/protobuf — Port und `OTEL_EXPORTER_OTLP_PROTOCOL`
müssen zusammenpassen, sonst überträgt nichts, **völlig geräuschlos**. Wir nehmen gRPC,
weil das der .NET-Default ist und es dort keinen Pfad gibt, den man vergessen kann.
(Der oft zitierte „Pfad `/v1/metrics` muss man selbst anhängen"-Fallstrick gilt **nicht**,
wenn der Endpunkt aus `OTEL_EXPORTER_OTLP_ENDPOINT` kommt — dann hängt das SDK ihn selbst
an. Nur wer `Endpoint` im Code setzt, muss den vollen Pfad angeben.)
Und: Temporalität nicht auf `delta` stellen. Spec-Default ist `cumulative`, und der
Prometheus-Exporter verwirft nicht-monotone Delta-Sums — also genau unseren Gauge.

**4.7 Metriknamen im Übergang OTel → Prometheus.** Die Übersetzung passiert **nicht** im
.NET-SDK (OTLP transportiert den Rohnamen 1:1), sondern erst im Prometheus-Exporter des
Collectors. Dessen Default-Strategie `UnderscoreEscapingWithSuffixes` zerlegt den Namen an
allen Zeichen außer `[a-zA-Z0-9:]` — also an Punkten **und** Unterstrichen —, hängt bei
monotonen Countern `_total` an und fügt Unit-Suffixe hinzu. Daraus folgen drei Regeln:

1. **Kein `_total` im Instrumentnamen.** Sonst wird aus `redepanda_messages_sent_total`
   am Ende `redepanda_messages_sent_total_total`.
2. **Keine Unit setzen** — oder nur eine Annotations-Unit in geschweiften Klammern
   (`{connections}`, `{messages}`). Besonders tückisch: Unit `"1"` an einem Gauge erzeugt
   `redepanda_active_connections_ratio`. Naheliegend gewählt, komplett falscher Name.
3. **Die Strategie explizit hinschreiben** statt sich auf den Default zu verlassen:
   `exporters.prometheus.translation_strategy: UnderscoreEscapingWithSuffixes`.
   (`add_metric_suffixes` ist dafür deprecated und wirkungslos, sobald `translation_strategy`
   gesetzt ist. `NoTranslation` ist offiziell als experimentell markiert — nicht nutzen.)

Zwei weitere Effekte, die beim ersten PromQL-Versuch für Verwirrung sorgen:

- **Prometheus überschreibt die Labels des Collectors**, wenn `honor_labels: true` fehlt —
  aus `job`/`instance` werden dann `exported_job`/`exported_instance` und die Abfrage aus
  dem README liefert leer.
- **Die ASP.NET-Core-HTTP-Metriken heißen anders als bei `prometheus-net`:**
  `http_server_request_duration_seconds_bucket/_sum/_count` statt
  `http_request_duration_seconds`, und `http_server_active_requests` statt
  `http_requests_in_progress`. Fürs Dashboard und den Screenshot relevant.
- Resource-Attribute landen als separate `target_info`-Metrik, nicht als Label an jeder
  Serie. Wer `pod=` direkt an `redepanda_messages_sent_total` sucht, findet nichts.
  `resource_to_telemetry_conversion.enabled: true` wäre der Schalter — brauchen wir aber
  nicht, weil `job` und `instance` ohnehin aus `service.name`/`service.instance.id` gesetzt
  werden.

---

## 5. README-Gliederung (Abgabe-relevant)

1. **Gruppenmitglieder** ← harte Anforderung, kommt nach ganz oben
2. Projektziel und Architekturdiagramm
3. Kommunikationswege Frontend ↔ Backend ↔ Redpanda
4. Voraussetzungen (Docker, .NET 9 SDK, kubectl, Helm, lokaler Cluster)
5. Lokal ohne Kubernetes (Compose + Konsolenclient)
6. Images bauen und in den Cluster laden
7. Installation mit Helm
8. Demoanleitung (Port-Forward, zwei Browserfenster, zwei Räume)
9. Konfiguration (Tabelle aus 4.5)
10. **Observability: OTel-SDK → Collector → Prometheus** — die vier fachlichen Metriken,
    PromQL-Beispiele, Screenshot, Hinweis auf die bewusste Beschränkung auf Metriken (E10)
11. Umgesetzte 12-Factor-Prinzipien — **mit ehrlicher Spalte „Einschränkung"**
12. Eingesetzte CNCF-Technologien und warum
13. Tests und manuelle Abnahme-Checkliste
14. Bekannte Einschränkungen
15. Fehlerbehebung

Punkt 2 („Architekturdiagramm") zeigt dasselbe Diagramm wie Abschnitt 0 dieses Plans und
muss den Metrikzweig mit abbilden.

### 12-Factor — geplanter Stand

Ehrlichkeit war in der Aufgabenstellung ausdrücklich erwünscht, deshalb die dritte Spalte.

| Faktor | Umsetzung | Einschränkung |
|---|---|---|
| Codebase | ein Git-Repo, ein Deployment-Chart | — |
| Dependencies | NuGet explizit; Frontend bewusst ohne Build-Tooling (Vanilla JS) | — |
| Config | ausschließlich Env-Variablen, im Cluster aus ConfigMap | — |
| Backing Services | Redpanda über `REDPANDA_BOOTSTRAP_SERVERS`, das Telemetrie-Backend über `OTEL_EXPORTER_OTLP_ENDPOINT` — beide als angehängte Ressourcen, beide ohne Codeänderung austauschbar | — |
| Build, Release, Run | Docker-Build → Helm-Release → Container-Start getrennt | manuell, kein CI (laut Aufgabe erlaubt) |
| Processes | kein lokaler Zustand; SSE-Verbindungen sind bewusst prozesslokal | Chatverlauf lebt nur im Kafka-Topic |
| Port Binding | Backend `:8080`, Frontend `:8080` (nginx-unprivileged), kein externer Webserver nötig | — |
| Concurrency | Consumer-GroupId pro Pod ⇒ echte Fan-out-Skalierung möglich | mit 1 Replica getestet, siehe E5 |
| Disposability | SIGTERM-Handling, Consumer/Producer sauber geschlossen | — |
| Dev/Prod Parity | identische Images lokal und im Cluster | Redpanda läuft im `dev-container`-Modus, einzelner Broker |
| Logs | stdout/stderr, keine Logdateien | Logs laufen bewusst **nicht** über OTLP; der Collector verarbeitet nur Metriken (E10) |
| Admin Processes | Topic-Anlage als Kubernetes-Job / Helm-Hook | — |

### CNCF-Technologien

- **Helm** (CNCF *graduated*) — die gesamte Anwendung wird darüber installiert,
  aktualisiert und deinstalliert; Parametrisierung über `values.yaml`.
- **OpenTelemetry** (CNCF *graduated* seit 11.05.2026 — davor seit 26.08.2021 *incubating*;
  nach Kubernetes das CNCF-Projekt mit der zweithöchsten Velocity) — das SDK im Backend
  erzeugt die vier fachlichen Metriken plus die ASP.NET-Core-HTTP-Instrumentierung und
  schickt sie per **OTLP** an den **OpenTelemetry Collector**. Die Anwendung ist dadurch
  an kein konkretes Monitoring-Backend gebunden.
  ⚠ Reifegrad ist **graduated**, nicht incubating. „Incubating" war bis Mai 2026 richtig und
  steht deshalb noch in fast jedem Tutorial — genau die Sorte Fehler, die man abschreibt.
  Vor Abgabe einmal auf `cncf.io/projects/opentelemetry/` gegenprüfen.
- **Prometheus** (CNCF *graduated*) — scrapt den Prometheus-Exporter des Collectors
  (`:8889`), speichert die Zeitreihen und liefert die Abfrageoberfläche für die Demo.
- **Kubernetes** (CNCF *graduated*) — Laufzeitplattform.
- Erwähnenswert: **Redpanda** ist ebenfalls in der CNCF-Landscape gelistet.
- Hinweis für die Doku: **Grafana** ist in der Landscape, aber *kein* CNCF-gehostetes
  Projekt. Falls wir es ergänzen, korrekt als solches beschreiben.

---

## 6. Abnahme-Checkliste (vor Abgabe komplett durchlaufen)

- [ ] `helm install` auf frischem Cluster → alle Pods `Ready` ohne manuellen Eingriff
- [ ] Zwei Browserfenster, gleicher Raum → beide sehen die Nachricht
- [ ] Zwei Browserfenster, verschiedene Räume → keine Vermischung
- [ ] Konsolenclient liest dieselben Nachrichten mit
- [ ] Leere Nachricht und Nachricht > `MAX_MESSAGE_LENGTH` → HTTP 400
- [ ] Frontend hat keinerlei Kafka-Zugriff (nachweisbar: nur `/api`-Aufrufe im Netzwerk-Tab)
- [ ] `kubectl delete pod <backend>` → Frontend verbindet sich neu, Chat läuft weiter
- [ ] Collector-Pod `Ready`, Collector-Logs ohne `permanent error` / `connection refused`
- [ ] Prometheus-Target `redepanda-otel-collector:8889` = `UP`
- [ ] `redepanda_messages_sent_total` und `redepanda_messages_received_total` steigen während
      der Demo; `redepanda_active_connections` fällt beim Schließen eines Browserfensters wieder
- [ ] Metriknamen exakt wie in der Aufgabenstellung — kein doppeltes `_total`, kein `_ratio`
- [ ] Backend-Pod hat **keinen** `/metrics`-Endpunkt mehr (`curl` liefert 404) — der Beleg,
      dass wirklich gepusht und nicht doch gescrapt wird
- [ ] `helm uninstall` entfernt alles bis auf das PVC (dokumentieren, dass das Absicht ist)
- [ ] README-Anleitung von einer unbeteiligten Person nachvollzogen
- [ ] Repo public, Gruppenmitglieder genannt

---

## 7. Nicht geplant

Bewusst außen vor, damit der Pflichtteil sicher fertig wird:
Argo CD, **Traces und Logs über OTLP** (der Collector könnte beides — wir aktivieren bewusst
nur die Metrics-Pipeline, siehe E10; Jaeger/Tempo als Trace-Backend entfällt damit ebenfalls),
**OpenTelemetry Operator / Auto-Instrumentation per Sidecar-Injection** (wir instrumentieren
manuell im Code), Service Mesh, Cloud-Deployment, Authentifizierung, persistenter
Chatverlauf mit Nachladen, Ingress (Port-Forward reicht für die Demo).

**Nur bei echtem Zeitüberschuss:** Grafana-Dashboard, Verlauf der letzten N Nachrichten
beim Betreten eines Raums, mehrere Backend-Replicas real testen, **Trace-Pipeline im
vorhandenen Collector aktivieren** (ein Receiver-/Exporter-Block plus
`service.pipelines.traces` — der billigste Zusatzpunkt, den wir noch hätten; zeigt, dass der
Collector eine Architekturentscheidung mit Ausbaupfad ist und kein Overhead).

---

## 8. Arbeitsteilung

Nach Phase 1 laufen zwei Stränge weitgehend unabhängig:

- **Strang A (Backend/Kafka):** Phase 2 Backend-Teil, **Phase 3 SDK-Seite** (NuGet, `Meter`,
  die vier Instrumente, OTLP-Exporter, Env-Variablen), Backend-Tests
- **Strang B (Frontend/Deployment):** Phase 2 Frontend-Teil, **Phase 3 Cluster-Seite**
  (Collector-Config, Prometheus-Deployment + Scrape-Config, `values.yaml`-Toggles,
  Probes/securityContext für beide neuen Pods), Helm-Feinschliff, Scripts
- **Vertrag zwischen den Strängen — vor Phase 3 festzurren:** Service-Name
  `redepanda-otel-collector`, Port `4317`, Protokoll gRPC, Instrumentnamen mit Punkten ohne
  `_total`. Solange der steht, kann Strang A gegen einen lokal per Docker gestarteten
  Collector entwickeln und Strang B gegen `telemetrygen` — ohne aufeinander zu warten.
  Ohne diesen Vertrag blockieren sich beide Stränge gegenseitig; das ist der einzige echte
  organisatorische Nachteil des Umbaus.
- **Gemeinsam:** Phase 0 und 1 zusammen — wer das Skeleton nicht mitgebaut hat,
  kann es später nicht debuggen. Phase 5 (README) ebenfalls gemeinsam.

---

## 9. Anhang — Collector-Config als Ausgangspunkt

Diese Config wurde gegen `otelcol` 0.158.0 validiert (`validate` Exit 0) und der Collector
damit unter `--read-only` als UID 10001 mit 256 MiB Limit betrieben. Nicht ungeprüft
übernehmen, aber als Startpunkt spart sie den halben Tag aus 4.6/4.7.

**Image: `otel/opentelemetry-collector:0.158.0` — die Core-Distribution, nicht contrib.**
Core enthält `prometheusexporter`, `prometheusremotewriteexporter` und `healthcheckextension`
(schon seit Jahren, nicht erst seit Kurzem). contrib zieht ~200 Komponenten nach, von denen
wir keine einzige brauchen. `otelcol-k8s` wäre trotz des Namens falsch — dort fehlt der
Prometheus-Exporter komplett. `args: ["--config=/conf/config.yaml"]` explizit setzen, das
Default-CMD zeigt auf einen anderen Pfad.

```yaml
extensions:
  health_check:            # ohne diesen Block ist Port 13133 tot → Probes schlagen fehl
    endpoint: 0.0.0.0:13133
    path: /

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317    # NICHT localhost — sonst kommt nichts aus anderen Pods an
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:          # MUSS der erste Processor sein (Backpressure Richtung Receiver)
    check_interval: 1s
    limit_mib: 160
    spike_limit_mib: 40
  batch:
    send_batch_size: 1024
    timeout: 10s

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
    translation_strategy: UnderscoreEscapingWithSuffixes   # explizit, siehe 4.7
    send_timestamps: true
    metric_expiration: 5m  # muss > OTEL_METRIC_EXPORT_INTERVAL sein, sonst flackern die Serien
    without_scope_info: true
    resource_to_telemetry_conversion:
      enabled: false
  debug:                   # heißt "debug", NICHT "logging" — das wurde in v0.111.0 entfernt
    verbosity: ${env:OTELCOL_DEBUG_VERBOSITY:-basic}

service:
  extensions: [health_check]      # health_check muss AUCH hier stehen
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheus, debug]
  telemetry:
    logs:
      level: ${env:OTELCOL_LOG_LEVEL:-info}
    metrics:
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
```

Passende `prometheus.yml` — der einzige Diff zum ursprünglichen Plan ist die Zieladresse:

```yaml
scrape_configs:
  - job_name: redepanda-otel
    honor_labels: true      # PFLICHT, sonst exported_job/exported_instance (siehe 4.7)
    scrape_interval: 5s     # 5s statt 15s, damit die Zähler in der Demo sichtbar steigen
    static_configs:
      - targets: ['redepanda-otel-collector:8889']

  - job_name: otel-collector          # Pipeline-Gesundheit des Collectors selbst
    static_configs:
      - targets: ['redepanda-otel-collector:8888']
```

**Debugging-Reihenfolge, wenn eine Metrik nicht ankommt** — von hinten nach vorn, sonst
sucht man an der falschen Stelle:

1. `kubectl -n redepanda logs deploy/redepanda-otel-collector` — kommt überhaupt etwas an?
   (`OTELCOL_DEBUG_VERBOSITY=detailed` setzen, dann wird jede Metrik geloggt)
2. `kubectl -n redepanda port-forward deploy/redepanda-otel-collector 8889:8889`
   → `curl localhost:8889/metrics` — steht der Name da, und heißt er richtig?
3. Prometheus-Targets-Seite — ist `redepanda-otel-collector:8889` auf `UP`?
4. Erst dann PromQL.

Für den Health-Port `13133` gegen `deploy/` forwarden, nicht gegen `svc/` — der Port steht
bewusst nicht im Service, und `kubectl port-forward svc/...` löst den Port über die
Service-Ports auf und bricht sonst ab.
