# RedePanda

Ein Browser-Chat, bei dem **Frontend** und **Backend** als zwei getrennte, selbst geschriebene
Deployments in Kubernetes laufen und über **Redpanda** (Kafka-Protokoll) miteinander sprechen —
installiert per **Helm**, instrumentiert per **OpenTelemetry** und ausgewertet in **Prometheus**.

![RedePanda](RedePanda.png)

---

## 1. Gruppenmitglieder

> ⚠️ **Vor der Abgabe ausfüllen.** Aus der Git-Historie sind bisher nur die GitHub-Konten
> bekannt; für die Abgabe werden die Klarnamen (und ggf. Matrikelnummern) benötigt.

| Name | GitHub |
|---|---|
| _(bitte eintragen)_ | [@deadmade](https://github.com/deadmade) |
| _(bitte eintragen)_ | [@DianaTin23](https://github.com/DianaTin23) |
| _(bitte eintragen)_ | [@maratin23](https://github.com/maratin23) |

---

## 2. Projektziel und Architektur

Zwei eigene Anwendungen, die **ausschließlich über Redpanda** Nachrichten austauschen. Das
Frontend kennt Kafka nicht, das Backend kennt Prometheus nicht — beides ist Absicht und lässt
sich in der Demo nachweisen.

```text
Browser ──HTTP──▶ Caddy (Frontend-Pod) ──proxy /api──▶ Backend-Pod ──Kafka──▶ Redpanda
   ▲                                                      │  │                (StatefulSet)
   └────────── SSE-Stream (/api/stream) ◀─────────────────┘  │
                                                             │ OTLP/gRPC :4317 (push)
                                                             ▼
                                                    OTel-Collector-Pod
                                                             │ :8889 /metrics
                                                             ▼ (scrape)
                                                      Prometheus-Pod
```

Der Metrikpfad zeigt bewusst **vom Backend weg**: die Anwendung *pusht*, sie wird nicht
gescrapt. Sie kennt Prometheus nicht, sondern nur einen OTLP-Endpunkt aus einer Env-Variable.
Das Backend hat deshalb **keinen `/metrics`-Endpunkt** — das ist in der Abnahmeliste unten
ausdrücklich nachprüfbar.

---

## 3. Kommunikationswege

| Strecke | Protokoll | Details |
|---|---|---|
| Browser → Frontend | HTTP | statische Dateien, `:8080` |
| Browser → Backend | HTTP über den Caddy-Proxy | alles unter `/api` |
| Backend → Browser | **SSE** (`text/event-stream`) | `GET /api/stream?room=X`, Heartbeat alle 15 s |
| Backend → Redpanda | Kafka | `ProduceAsync`, Key = Raumname |
| Redpanda → Backend | Kafka | Consumer-Gruppe pro Pod, `AutoOffsetReset.Latest` |
| Backend → Collector | OTLP/gRPC `:4317` | Push, alle 5 s |
| Prometheus → Collector | HTTP-Scrape `:8889` | alle 5 s |

**Warum SSE und nicht SignalR:** SignalR bräuchte ein WebSocket-Upgrade durch den Proxy,
Sticky Sessions und ab der zweiten Replica ein Backplane. Für einen reinen
Server→Browser-Broadcast genügt SSE, funktioniert durch jeden Proxy und kommt ohne
Client-Bibliothek aus.

**Warum ein Topic für alle Räume:** Der Raum steht als Feld *und* als Kafka-Key in der
Nachricht. Ein Topic heißt ein Init-Job; der Key sichert die Reihenfolge pro Raum, falls das
Topic je mehr Partitionen bekommt. Gefiltert wird serverseitig im Backend.

---

## 4. Voraussetzungen

| Werkzeug | Version | Wofür |
|---|---|---|
| Docker oder Podman | — | Images bauen, Redpanda lokal |
| .NET SDK | 9.0 | Backend, Client, Tests |
| kubectl | 1.3x | Cluster-Zugriff |
| Helm | 3 oder 4 | Installation (entwickelt gegen **4.2.3**) |
| lokaler Cluster | kind, minikube oder Docker Desktop | Laufzeitumgebung |

Wer **Nix** benutzt, bekommt alles über die mitgelieferte Dev-Shell:

```bash
nix develop        # oder: direnv allow
```

Diese Shell liefert .NET 9, `rpk`, `kubectl`, `helm`, `kubeconform` und `docker-compose`.

---

## 5. Lokal ohne Kubernetes

Der schnellste Weg, den Chat zu sehen — nur Redpanda im Container, alles andere direkt.

```bash
# 1. Broker starten
cd RedePanda-kafka-docker
docker compose --env-file env.local up -d

# 2. Topic anlegen
rpk topic create redepanda-chat -p 1 -r 1 -X brokers=127.0.0.1:19092

# 3. Backend starten
REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
OTEL_SDK_DISABLED=true \
ASPNETCORE_URLS=http://127.0.0.1:5080 \
dotnet run --project src/RedePanda.Backend

# 4. Konsolenclient in einem zweiten Terminal
REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
dotnet run --project src/RedePanda.ChatClient -- --nick alice --room general
```

Eine Nachricht über das Backend schicken und im Client mitlesen:

```bash
curl -X POST http://127.0.0.1:5080/api/messages \
  -H 'content-type: application/json' \
  -d '{"room":"general","nickname":"web","text":"hallo"}'
```

Der Konsolenclient liest **absichtlich alle Räume** mit. Das ist der einfachste Beleg dafür,
dass Backend und Client wirklich dasselbe Kafka-Topic benutzen.

---

## 6. Images bauen und in den Cluster laden

```bash
./scripts/build-images.sh                  # nur bauen
./scripts/build-images.sh --load kind      # bauen + in kind laden
./scripts/build-images.sh --load minikube  # bauen + in minikube laden
```

Lokal gebaute Images kennt der Cluster sonst nicht (`ImagePullBackOff`). Deshalb steht in den
Manifesten `imagePullPolicy: IfNotPresent` **und** die Images müssen explizit geladen werden:

| Cluster | Befehl |
|---|---|
| kind | `kind load docker-image redepanda-backend:dev redepanda-frontend:dev` |
| kind + Podman | `podman save` → `kind load image-archive` (macht das Skript automatisch) |
| minikube | `minikube image load redepanda-backend:dev redepanda-frontend:dev` |
| Docker Desktop | nichts nötig — gemeinsamer Image-Store |

> `imagePullPolicy: Never` wäre hier **schlechter**: es scheitert mit `ErrImageNeverPull`,
> statt auf einen Pull zurückzufallen, und bricht damit den Docker-Desktop-Weg.

---

## 7. Installation mit Helm

```bash
helm upgrade --install redepanda ./deploy/helm/redepanda \
  -n redepanda --create-namespace --wait --timeout 10m

kubectl -n redepanda get pods
```

Ohne Helm geht es auch — das gerenderte Manifest liegt im Repo:

```bash
kubectl create namespace redepanda
kubectl -n redepanda apply -f deploy/k8s/rendered.yaml
```

Deinstallation:

```bash
helm uninstall redepanda -n redepanda
```

> Das **PVC des Brokers bleibt absichtlich stehen.** PVCs aus `volumeClaimTemplates` gehören
> dem StatefulSet-Controller, nicht dem Helm-Release; Kubernetes löscht sie bewusst nicht mit.
> Wer wirklich bei null anfangen will: `kubectl -n redepanda delete pvc --all`.

### Schalter in `values.yaml`

| Wert | Default | Wirkung |
|---|---|---|
| `otelCollector.enabled` | `true` | Collector-Pod; ausgeschaltet setzt die ConfigMap `OTEL_SDK_DISABLED=true` |
| `prometheus.enabled` | `true` | Prometheus-Pod. **Setzt `otelCollector.enabled` voraus** |
| `backend.replicas` | `1` | siehe „Bekannte Einschränkungen" |
| `chat.topic` | `redepanda-chat` | Topicname für Backend, Client und Init-Job |
| `chat.maxMessageLength` | `500` | maximale Nachrichtenlänge |

Prometheus ohne Collector ist sinnlos — der Collector ist das einzige Scrape-Ziel. Diese
Kombination bricht das Rendern bewusst mit einer klaren Meldung ab, statt einen Prometheus zu
installieren, der ins Leere scrapt.

---

## 8. Demo

```bash
./scripts/demo.sh
```

Öffnet die Port-Forwards für Frontend (8080), Prometheus (9090) und Collector (8889).

1. **Zwei Browserfenster** auf <http://localhost:8080>, beide Raum `general`, verschiedene
   Namen → beide sehen jede Nachricht.
2. **Zweiter Raum:** ein Fenster auf `andererraum` → keine Vermischung.
3. **Netzwerk-Tab öffnen:** das Frontend spricht ausschließlich mit `/api/...`. Kein Kafka.
4. **Konsolenclient** parallel laufen lassen → liest dieselben Nachrichten mit.
5. **Prometheus** auf <http://localhost:9090>, Abfrage `redepanda_messages_sent_total` →
   steigt sichtbar mit.
6. **Backend-Pod löschen** (`kubectl -n redepanda delete pod -l app.kubernetes.io/component=backend`)
   → das Frontend zeigt kurz „verbinde neu…", blendet einen Hinweis über dem Eingabefeld ein und
   sperrt es, bis der Stream wieder steht. Danach läuft der Chat weiter.

### Was im Frontend absichtlich so ist

- **Kein Verlauf.** Der Consumer liest ab `AutoOffsetReset.Latest`, es gibt keinen History-Endpunkt.
  Wer einen Raum betritt, sieht deshalb zuerst einen leeren Zustand, der genau das erklärt — die
  Anwendung ist nicht kaputt, sie zeigt nur ab jetzt.
- **Kein Build-Tooling, keine externen Requests.** Kein npm, kein CDN, keine Webfonts: das Frontend
  besteht aus vier statischen Dateien, die Caddy ausliefert. Im Netzwerk-Tab tauchen nur diese
  Dateien und `/api/...` auf — das ist die Grundlage für Punkt 3 oben und funktioniert auch in einem
  Cluster ohne Internetzugang.
- **Hell und dunkel.** Standardmäßig folgt die Oberfläche der Systemeinstellung; der Schalter oben
  rechts erzwingt ein Schema und merkt es sich. Praktisch für Screenshots, ohne das Betriebssystem
  umzustellen.
- **Farbe pro Name.** Der Farbton wird aus dem Nickname gehasht; Helligkeit und Sättigung kommen aus
  dem Theme, damit jeder erzeugte Farbton auf beiden Hintergründen lesbar bleibt. Eigene Nachrichten
  erkennt man an Position und Label „(du)", nicht an der Farbe allein.

---

## 9. Konfiguration

Alle Einstellungen kommen aus Umgebungsvariablen; im Cluster aus einer ConfigMap.

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

Die ersten sechs liest das Backend **explizit** in `BackendOptions` aus, statt sich auf das
`Section__Key`-Autobinding von ASP.NET zu verlassen — nur so stimmen die schlichten Namen aus
dieser Tabelle mit dem Code überein.

Die `OTEL_*`-Variablen sind dagegen in der OpenTelemetry-Spezifikation genormt und werden vom
SDK selbst gelesen. Sie werden im Code **bewusst nicht** nachgebaut: zwei Wahrheiten wären
schlimmer als eine fremde Konvention.

---

## 10. Observability: OTel-SDK → Collector → Prometheus

Das Backend erzeugt vier fachliche Metriken über `Meter("RedePanda")` und schickt sie per OTLP
an den Collector. Der Collector übersetzt die Namen und stellt sie unter `:8889` bereit;
Prometheus scrapt **nur den Collector**.

| Instrument in C# | Typ | Name in Prometheus |
|---|---|---|
| `redepanda.messages.sent` | `Counter<long>` | `redepanda_messages_sent_total` |
| `redepanda.messages.received` | `Counter<long>` | `redepanda_messages_received_total` |
| `redepanda.kafka.errors` | `Counter<long>` | `redepanda_kafka_errors_total` |
| `redepanda.active_connections` | `ObservableUpDownCounter<int>` | `redepanda_active_connections` |

### PromQL für die Demo

```promql
redepanda_messages_sent_total
rate(redepanda_messages_received_total[1m])
redepanda_active_connections
histogram_quantile(0.95, rate(http_server_request_duration_seconds_bucket[1m]))
```

### Beleg, dass der Weg wirklich über den Collector führt

Der reine Anstieg eines Zählers würde auch bei direktem Scraping so aussehen. Zwei Nachweise:

```bash
# 1. Prometheus-Targets: redepanda-otel-collector:8889 muss UP sein
open http://localhost:9090/targets

# 2. Das Backend hat gar keinen /metrics-Endpunkt
kubectl -n redepanda port-forward deploy/redepanda-backend 5080:8080
curl -i http://localhost:5080/metrics     # -> 404
```

### Warum ein Collector und nicht `prometheus-net`

Das Backend kennt kein Monitoring-Backend, sondern nur einen OTLP-Endpunkt aus einer
Env-Variable — dieselbe 12-Factor-Argumentation wie bei Redpanda. Das Monitoring-Backend lässt
sich ohne Rebuild der Anwendung austauschen. Preis: ein Pod und ein Netzwerk-Hop mehr.

### Bewusst nur Metriken

Der Collector könnte auch Traces und Logs. Aktiviert ist **nur die Metrics-Pipeline**. Traces
bräuchten Kontextpropagierung von Hand über die Kafka-Grenze — `Confluent.Kafka` hat keine
stabile Auto-Instrumentierung — plus ein zweites Backend. Logs gehen nach stdout (12-Factor).
Die Trace-Pipeline nachzurüsten wäre im vorhandenen Collector ein Receiver- und ein
Exporter-Block; der Ausbaupfad steht offen.

---

## 11. Umgesetzte 12-Factor-Prinzipien

| Faktor | Umsetzung | Einschränkung |
|---|---|---|
| Codebase | ein Git-Repo, ein Deployment-Chart | — |
| Dependencies | NuGet explizit; Frontend bewusst ohne Build-Tooling (Vanilla JS) | — |
| Config | ausschließlich Env-Variablen, im Cluster aus ConfigMap | — |
| Backing Services | Redpanda über `REDPANDA_BOOTSTRAP_SERVERS`, Telemetrie-Backend über `OTEL_EXPORTER_OTLP_ENDPOINT` — beide ohne Codeänderung austauschbar | — |
| Build, Release, Run | Docker-Build → Helm-Release → Container-Start getrennt | manuell, kein CI |
| Processes | kein lokaler Zustand; SSE-Verbindungen sind bewusst prozesslokal | Chatverlauf lebt nur im Kafka-Topic |
| Port Binding | Backend `:8080`, Frontend `:8080`, kein externer Webserver nötig | — |
| Concurrency | Consumer-GroupId pro Pod ⇒ echte Fan-out-Skalierung möglich | nur mit 1 Replica getestet |
| Disposability | SIGTERM: Consumer `Close()`, Producer `Flush()`, Grace Period 45 s > App-Timeout 25 s | — |
| Dev/Prod Parity | identische Images lokal und im Cluster | Redpanda läuft im `dev-container`-Modus, einzelner Broker |
| Logs | strukturiert (JSON) nach stdout, keine Logdateien | Logs laufen bewusst **nicht** über OTLP |
| Admin Processes | Topic-Anlage als Kubernetes-Job / Helm-Hook | — |

---

## 12. Eingesetzte CNCF-Technologien

- **Kubernetes** (*graduated*) — Laufzeitplattform.
- **Helm** (*graduated*) — die gesamte Anwendung wird darüber installiert, aktualisiert und
  deinstalliert; Parametrisierung über `values.yaml`.
- **OpenTelemetry** (*graduated* seit **11.05.2026**, angekündigt am 21.05.2026; davor seit
  26.08.2021 *incubating*) — das SDK im Backend erzeugt die vier fachlichen Metriken plus die
  ASP.NET-Core-HTTP-Instrumentierung und schickt sie per **OTLP** an den
  **OpenTelemetry Collector**.
- **Prometheus** (*graduated*) — scrapt den Prometheus-Exporter des Collectors, speichert die
  Zeitreihen und liefert die Abfrageoberfläche.

Ergänzend: **Redpanda** ist in der CNCF-Landscape gelistet, aber **kein** CNCF-gehostetes
Projekt und nicht Open Source im engeren Sinne (BSL 1.1, Apache-2.0 nach vier Jahren).
**Grafana** wäre ebenfalls Landscape, aber nicht CNCF-gehostet — hier nicht eingesetzt.

---

## 13. Tests und Abnahme-Checkliste

```bash
dotnet test                                    # 31 Tests
helm lint deploy/helm/redepanda
helm template redepanda deploy/helm/redepanda -n redepanda \
  | kubeconform -strict -summary -kubernetes-version 1.32.0
```

> `kubectl apply --dry-run=client` eignet sich hierfür **nicht**: es kontaktiert trotz
> „client" einen API-Server, um Ressourcentypen aufzulösen, und scheitert ohne Cluster.

### Automatisiert abgedeckt

Validierung (leer/Whitespace/zu lang, Grenzwerte), Wire-Format-Roundtrip inklusive Unicode,
Ablehnung defekter Payloads, serverseitiger Zeitstempel und Raumtrennung im Broadcaster.

### Manuell durchzugehen

- [ ] `helm install` auf frischem Cluster → alle Pods `Ready` ohne Eingriff
- [ ] Topic-Job `Completed`
- [ ] Zwei Browserfenster, gleicher Raum → beide sehen die Nachricht
- [ ] Zwei Browserfenster, verschiedene Räume → keine Vermischung
- [ ] Konsolenclient liest dieselben Nachrichten mit
- [ ] Leere Nachricht und Nachricht > `MAX_MESSAGE_LENGTH` → HTTP 400
- [ ] Frontend hat keinerlei Kafka-Zugriff (Netzwerk-Tab: nur `/api`)
- [ ] `kubectl delete pod <backend>` → Frontend verbindet neu, Chat läuft weiter
- [ ] Collector-Pod `Ready`, Logs ohne `permanent error` / `connection refused`
- [ ] Prometheus-Target `redepanda-otel-collector:8889` = `UP`
- [ ] `redepanda_messages_sent_total` steigt; `redepanda_active_connections` fällt beim
      Schließen eines Fensters
- [ ] Backend hat **keinen** `/metrics`-Endpunkt (`curl` → 404)
- [ ] `helm uninstall` entfernt alles bis auf das PVC
- [ ] README von einer unbeteiligten Person nachvollzogen
- [ ] Repository public, Gruppenmitglieder eingetragen

---

## 14. Bekannte Einschränkungen

- **Eine Backend-Replica.** Mehr Replicas funktionieren technisch — jeder Pod konsumiert unter
  eigener Group-ID und sieht daher jede Nachricht —, aber ein Browser hängt immer an genau
  einem Pod. Getestet ist nur `replicas: 1`.
- **Kein Chatverlauf.** Wer einen Raum betritt, sieht nur ab jetzt (`AutoOffsetReset.Latest`).
  Die Nachrichten liegen im Topic; sie werden nur nicht nachgeladen.
- **Redpanda im `dev-container`-Modus**, ein einzelner Broker ohne Replikation. Für eine Demo
  richtig, für Produktion nicht.
- **Der Broker-Pod läuft ohne `readOnlyRootFilesystem`**, als einziger im Chart:
  `rpk redpanda start` schreibt bei jedem Start die zusammengeführte Konfiguration nach
  `/etc/redpanda/redpanda.yaml`.
- **Kein Ingress, keine Authentifizierung, kein TLS.** Port-Forward genügt für die Demo.
- **.NET 9 ist STS** und läuft am 11.11.2026 aus — nach dem Abgabetermin.
- **Kein CI.**

### Was in dieser Session *nicht* auf einem echten Cluster verifiziert wurde

Ehrlichkeitshalber: auf dem Entwicklungsrechner war kein Cluster verfügbar (rootless Podman
ohne `cpuset`-Delegation, kein kubectl-Kontext). Verifiziert wurden Build, Tests, beide Images,
der komplette Chat gegen ein echtes Redpanda, der komplette Metrikpfad gegen einen echten
OTel-Collector sowie `helm lint`, `helm template` und `kubeconform --strict` für alle drei
Wertekombinationen.

**Nicht** verifiziert und beim ersten echten Deployment zuerst zu prüfen: Pods erreichen
`Ready`, der Topic-Job läuft durch, Prometheus-Target `UP`, `honor_labels`-Verhalten,
Pod-Neustart-Resilienz und dass `helm uninstall` das PVC stehen lässt.

---

## 15. Fehlerbehebung

**Metrik kommt nicht an — von hinten nach vorn suchen:**

```bash
# 1. Kommt überhaupt etwas am Collector an?
kubectl -n redepanda logs deploy/redepanda-otel-collector
#    ausführlicher: --set otelCollector.debugVerbosity=detailed

# 2. Steht der Name da, und heißt er richtig?
kubectl -n redepanda port-forward deploy/redepanda-otel-collector 8889:8889
curl localhost:8889/metrics | grep redepanda_

# 3. Ist das Target UP?  http://localhost:9090/targets
# 4. Erst dann PromQL.
```

Für den Health-Port `13133` gegen `deploy/...` forwarden, **nicht** gegen `svc/...` — der Port
steht bewusst nicht im Service, und `port-forward svc/...` löst Ports über die Service-Ports auf.

| Symptom | Ursache |
|---|---|
| `ImagePullBackOff` | Images nicht in den Cluster geladen → Abschnitt 6 |
| Broker `CrashLoopBackOff`, „Argument parse error" | `command:` überschreibt den Entrypoint. Muss `[rpk, redpanda, start]` sein |
| Topic-Job läuft in den Timeout | `rpk cluster health` braucht `-X admin.hosts=redpanda:9644`, **nicht** `--brokers` |
| `helm upgrade` scheitert am Topic-Job | `rpk topic create` beendet sich mit 1, wenn das Topic existiert — der Job fängt das ab |
| Prometheus startet nicht, „non-numeric user (nobody)" | `runAsUser: 65534` fehlt |
| Broker startet, schreibt nicht | `fsGroup: 101` fehlt; frisches PVC gehört sonst root |
| PromQL mit `instance=` ist leer | `instance` ist eine GUID → im Code wurde `AddService()` gesetzt und schlägt `OTEL_RESOURCE_ATTRIBUTES` |
| `exported_job` / `exported_instance` in Prometheus | `honor_labels: true` fehlt im Scrape-Config |
| Nachrichten kommen verzögert | Proxy puffert. Caddy tut das bei SSE nicht — nginx schon |
| `DllNotFoundException` beim Start | Backend-Image auf Alpine/Chiseled gebaut; librdkafka braucht glibc |

---

## Projektstruktur

```text
src/RedePanda.Contracts/    ChatMessage + Validierung + Wire-Format (geteilt)
src/RedePanda.Backend/      ASP.NET Core: SSE, Kafka, OpenTelemetry
src/RedePanda.Frontend/     Caddyfile + Vanilla-JS-Frontend (index.html, style.css, app.js, favicon.svg)
src/RedePanda.ChatClient/   Konsolenclient
tests/                      xUnit
deploy/helm/redepanda/      Helm-Chart
deploy/k8s/rendered.yaml    generiert via `helm template`
scripts/                    build-images.sh, demo.sh
RedePanda-kafka-docker/     Redpanda für lokale Entwicklung ohne Kubernetes
```
