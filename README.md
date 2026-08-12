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
| Backend → Browser | **SSE** (`text/event-stream`) | `GET /api/stream?room=X`, Verlauf zuerst, Heartbeat alle 15 s |
| Backend → Redpanda | Kafka | `ProduceAsync`, Key = Raumname |
| Redpanda → Backend | Kafka | Consumer-Gruppe pro Pod, `AutoOffsetReset.Earliest` (Verlauf) |
| Backend → Collector | OTLP/gRPC `:4317` | Push, alle 5 s |
| Prometheus → Collector | HTTP-Scrape `:8889` | alle 5 s |

**Warum SSE und nicht SignalR:** SignalR bräuchte ein WebSocket-Upgrade durch den Proxy,
Sticky Sessions und ab der zweiten Replica ein Backplane. Für einen reinen
Server→Browser-Broadcast genügt SSE, funktioniert durch jeden Proxy und kommt ohne
Client-Bibliothek aus.

**Und es braucht auch bei mehreren Replicas keins von beidem.** Die `id` jedes Datenframes ist der
Kafka-Offset — der gehört dem Broker, nicht einem einzelnen Pod. Ein Browser, dessen Verbindung
abreißt und der auf einer *anderen* Replica wieder aufsetzt, schickt denselben `Last-Event-ID` mit
und bekommt genau das, was er verpasst hat. Das Backplane ist Redpanda selbst, und `sessionAffinity`
am Service wäre nicht nur unnötig, sondern schädlich: es würde einen Rollout künstlich verlängern.

**Warum ein Topic für alle Räume:** Der Raum steht als Feld *und* als Kafka-Key in der
Nachricht. Ein Topic heißt ein Init-Job; der Key sichert die Reihenfolge pro Raum, falls das
Topic je mehr Partitionen bekommt. Gefiltert wird serverseitig im Backend.

---

## 4. Voraussetzungen

| Werkzeug | Version | Wofür |
|---|---|---|
| Docker oder Podman | — | Images bauen, Redpanda lokal |
| .NET SDK | 10.0 | Backend, Client, Tests |
| kubectl | 1.3x | Cluster-Zugriff |
| Helm | 3 oder 4 | Installation (entwickelt gegen **4.2.3**) |
| lokaler Cluster | kind, minikube oder Docker Desktop | Laufzeitumgebung |

Wer **Nix** benutzt, bekommt alles über die mitgelieferte Dev-Shell:

```bash
nix develop        # oder: direnv allow
```

Diese Shell liefert .NET 10, `rpk`, `kubectl`, `helm`, `kubeconform`, `skopeo` und
`docker-compose`.

### Zentrale Build-Konfiguration

Die vier Projekte legen weder Zielframework noch Paketversionen selbst fest. Beides steht im
Wurzelverzeichnis, sodass ein Upgrade jeweils **eine** Datei betrifft:

| Datei | Inhalt |
|---|---|
| `global.json` | SDK-Feature-Band (10.0.302, `rollForward: latestPatch`) |
| `Directory.Build.props` | `TargetFramework`, `Nullable`, `ImplicitUsings`, `TreatWarningsAsErrors`, `RestorePackagesWithLockFile` |
| `Directory.Packages.props` | Alle direkten NuGet-Versionen (Central Package Management) |
| `NuGet.config` | Die einzige zugelassene Paketquelle (`<clear />` + nuget.org) |
| `*/packages.lock.json` | Der vollständig aufgelöste Graph inklusive **transitiver** Pakete, mit Content-Hashes |

Die `.csproj`-Dateien enthalten deshalb nur noch `<PackageReference Include="…" />` **ohne**
`Version`. Wer eine Version dort doch angibt, bricht den Restore — das ist beabsichtigt.

#### Reproduzierbarkeit: Lockfiles und Digests

`Directory.Packages.props` pinnt nur, was direkt referenziert wird. Was diese Pakete ihrerseits
mitbringen, stand vorher frei — im Testprojekt allein 52 transitive Pakete. Die committeten
`packages.lock.json` schließen diese Lücke.

- **Version geändert?** Ein normales `dotnet restore` schreibt die Lockfile neu. Das Ergebnis
  gehört mit in den Commit.
- **Container-Build:** `dotnet restore --locked-mode` — eine nicht mehr passende Lockfile wird
  zum Build-Fehler (`NU1004`) statt zu einem stillen Upgrade.

`global.json` ist bewusst auf **10.0.302** gepinnt, dasselbe Feature-Band, das die Nix-Shell
liefert und auf das das SDK-Image im Backend-`Dockerfile` festgelegt ist. `latestPatch` lässt
nur noch Patch-Stände zu. Ein Band-Sprung (10.0.4xx) ist damit eine bewusste Änderung an
`flake.nix`, `global.json` und dem `FROM` des Build-Stages — nicht länger etwas, das ein
beliebiger Rechner nebenbei mitbringt.

Jedes Image aus einer Registry ist zusätzlich per **Digest** gepinnt, nicht nur per Tag: ein
Tag kann auf anderen Inhalt umgehängt werden, ein Digest nicht. Gepinnt wird jeweils der
Digest der Manifest-Liste, damit arm64 genauso funktioniert wie amd64.

```bash
./scripts/check-digests.sh    # meldet, wenn ein Tag von seinem Digest weggewandert ist
```

Das Skript schreibt nichts um; es gibt die Ersatzzeile aus. Nach einer Änderung an
`values.yaml` muss `deploy/k8s/rendered.yaml` neu erzeugt werden (siehe unten).

Ausgenommen sind `redepanda-backend` und `redepanda-frontend`: die baut
`scripts/build-images.sh` lokal, sie werden nie aus einer Registry geladen, und ein Digest
würde `imagePullPolicy: IfNotPresent` brechen. Bei ihnen übernimmt der **Tag** die Aufgabe des
Digests: er wird aus `appVersion` und dem Git-Commit abgeleitet (`0.1.0-g103b98b`) und nie
wiederverwendet — ein Tag, ein Build. Siehe Abschnitt 6.

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
./scripts/build-images.sh --release --load kind   # Release schneiden (nur aus sauberem Baum)
```

Der Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus `Chart.yaml` plus der kurze
Git-Commit, also `redepanda-backend:0.1.0-g103b98b`. Er wird nie wiederverwendet. Am Ende
schreibt das Skript die dazugehörige **Release-Datei** und gibt den Deploy-Befehl aus:

```text
==> Built redepanda-backend:0.1.0-g103b98b and redepanda-frontend:0.1.0-g103b98b
==> Wrote deploy/releases/0.1.0-g103b98b.yaml

Deploy this release:

  helm upgrade --install redepanda deploy/helm/redepanda \
    -n redepanda --create-namespace --wait --timeout 10m \
    -f deploy/releases/0.1.0-g103b98b.yaml \
    --description "release 0.1.0-g103b98b"
```

Bei einem **unsauberen Arbeitsbaum** warnt das Skript und hängt einen Hash des uncommitteten
Standes an (`0.1.0-g103b98b-dirty.5f4f110`) — auch dieser Tag bleibt damit eindeutig, aber die
Datei ist per `.gitignore` von einem Release ausgenommen. `--release` bricht in dem Fall ab.

Lokal gebaute Images kennt der Cluster sonst nicht (`ImagePullBackOff`). Deshalb steht in den
Manifesten `imagePullPolicy: IfNotPresent` **und** die Images müssen explizit geladen werden:

| Cluster | Befehl |
|---|---|
| kind | `kind load docker-image redepanda-backend:$TAG redepanda-frontend:$TAG` |
| kind + Podman | `podman save` → `kind load image-archive` (macht das Skript automatisch) |
| minikube | `minikube image load redepanda-backend:$TAG redepanda-frontend:$TAG` |
| Docker Desktop | nichts nötig — gemeinsamer Image-Store |

`$TAG` ist die abgeleitete Version; mit `--load` erledigt das Skript diesen Schritt selbst.

> `imagePullPolicy: Never` wäre hier **schlechter**: es scheitert mit `ErrImageNeverPull`,
> statt auf einen Pull zurückzufallen, und bricht damit den Docker-Desktop-Weg.

---

## 7. Installation mit Helm

```bash
helm upgrade --install redepanda ./deploy/helm/redepanda \
  -n redepanda --create-namespace --wait --timeout 10m \
  -f deploy/releases/0.1.0-g103b98b.yaml \
  --description "release 0.1.0-g103b98b"

kubectl -n redepanda get pods
```

Die Release-Datei ist **Pflicht**, nicht optional. Ohne sie bricht das Chart mit einer klaren
Meldung ab, statt irgendein Image zu starten:

```text
Error: backend.image.tag is empty: no release selected.
Run scripts/build-images.sh, then deploy with -f deploy/releases/<version>.yaml
```

Was läuft gerade?

```bash
kubectl -n redepanda get pods -L app.kubernetes.io/version
kubectl -n redepanda get deploy redepanda-backend -o jsonpath='{..image}'
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

### Rollback

Jede Installation und jedes Upgrade ist eine Helm-Revision. Weil jede Revision einen eigenen,
unveränderlichen Image-Tag mitbringt, holt ein Rollback wirklich den alten Stand zurück:

```bash
helm history redepanda -n redepanda
# REVISION  STATUS      DESCRIPTION
# 1         superseded  release 0.1.0-ga1b2c3d
# 2         deployed    release 0.1.0-g103b98b

helm rollback redepanda 1 -n redepanda
kubectl -n redepanda get deploy redepanda-frontend -o jsonpath='{..image}'
# redepanda-frontend:0.1.0-ga1b2c3d
```

Die Spalte `DESCRIPTION` kommt aus `--description` im Deploy-Befehl. `APP VERSION` taugt dafür
**nicht**: die liest Helm aus `Chart.yaml` und sie ist auf jeder Revision dieselbe.

> Voraussetzung ist, dass das alte Image noch im Image-Store der Node liegt. Es gibt keine
> Registry — wurde es weggeräumt, baut man es aus dem Commit neu, der in der Release-Datei
> unter `release.gitSha` steht. Das ist die bewusste Grenze der Registry-losen Variante.

### Schalter in `values.yaml`

| Wert | Default | Wirkung |
|---|---|---|
| `otelCollector.enabled` | `true` | Collector-Pod; ausgeschaltet setzt die ConfigMap `OTEL_SDK_DISABLED=true` |
| `prometheus.enabled` | `true` | Prometheus-Pod. **Setzt `otelCollector.enabled` voraus** |
| `backend.replicas` | `2` | Backend-Pods. Ab 2 überlebt der Chat den Ausfall eines Pods; wird ignoriert, solange `backend.autoscaling.enabled` gesetzt ist |
| `backend.autoscaling.enabled` | `false` | HPA auf CPU-Basis. **Braucht metrics-server**, siehe unten |
| `chat.topic` | `redepanda-chat` | Topicname für Backend, Client und Init-Job |
| `chat.maxMessageLength` | `500` | maximale Nachrichtenlänge |
| `chat.historySize` | `200` | Nachrichten pro Raum im Speicher **jedes** Pods; `0` = alles, was im Topic liegt |

Prometheus ohne Collector ist sinnlos — der Collector ist das einzige Scrape-Ziel. Diese
Kombination bricht das Rendern bewusst mit einer klaren Meldung ab, statt einen Prometheus zu
installieren, der ins Leere scrapt.

### HPA einschalten (braucht metrics-server)

Der HorizontalPodAutoscaler ist bewusst **aus**. Er misst gegen `metrics.k8s.io`, und das liefert
nur ein Cluster mit metrics-server — den weder kind noch Docker Desktop mitbringen. Ohne ihn stünde
das Objekt zwar da, zeigte aber dauerhaft `<unknown>/70%` und skalierte nie. Das sieht kaputter aus
als gar kein HPA, deshalb ist er ein Schalter und keine Voraussetzung.

```bash
# minikube: Addon
minikube addons enable metrics-server

# kind / Docker Desktop: kein Addon. Der zweite Befehl ist nötig, weil die kubelet-Zertifikate
# dieser Cluster nicht von der Cluster-CA signiert sind.
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl -n kube-system rollout status deploy/metrics-server

# Das Tor: erst wenn hier Zahlen stehen, lohnt der HPA.
kubectl top pods -n redepanda

helm upgrade redepanda ./deploy/helm/redepanda -n redepanda -f "$REL" \
  --set backend.autoscaling.enabled=true
kubectl -n redepanda get hpa redepanda-backend
```

`TARGETS` muss eine Zahl sein, kein `<unknown>`. Ist der HPA an, lässt das Deployment `replicas:`
weg — sonst schriebe jedes `helm upgrade` den Wert aus `values.yaml` zurück und der Autoscaler
korrigierte ihn sofort wieder.

---

## 8. Demo

```bash
./scripts/demo.sh
```

Öffnet die Port-Forwards für Frontend (8080), Prometheus (9090) und Collector (8889).

1. **Zwei Browserfenster** auf <http://localhost:8080>, beide Raum `general`, verschiedene
   Namen → beide sehen jede Nachricht.
2. **Zweiter Raum:** ein Fenster auf `andererraum` → keine Vermischung.
3. **Seite neu laden:** der Verlauf des Raums ist wieder da — er kommt aus dem Topic, nicht aus dem
   Browser. Ein drittes Fenster, das den Raum zum ersten Mal betritt, sieht dasselbe.
4. **Netzwerk-Tab öffnen:** das Frontend spricht ausschließlich mit `/api/...`. Kein Kafka.
5. **Konsolenclient** parallel laufen lassen → liest dieselben Nachrichten mit.
6. **Prometheus** auf <http://localhost:9090>, Abfrage `redepanda_messages_sent_total` →
   steigt sichtbar mit.
7. **Einen von zwei Backend-Pods löschen.** Vier Fenster im selben Raum öffnen (bei zweien ist es
   ein Münzwurf, ob sie überhaupt auf verschiedenen Pods landen — kube-proxy entscheidet das pro
   TCP-Verbindung), dann:
   ```bash
   kubectl -n redepanda get pods -l app.kubernetes.io/component=backend
   kubectl -n redepanda delete pod <einer der beiden>
   ```
   Die Fenster am **anderen** Pod merken davon nichts. Die am gelöschten verbinden neu, landen auf
   der überlebenden Replica und lesen dort weiter — **ohne** dass der bereits gelesene Verlauf ein
   zweites Mal erscheint. Das ist der Beleg, dass die Skalierung echt ist und nicht behauptet: der
   Offset in `Last-Event-ID` gilt brokerweit, also auch auf einem Pod, der die Verbindung nie
   gesehen hat.
8. **Rollout ohne Ausfall.** Währenddessen weitertippen:
   ```bash
   kubectl -n redepanda rollout restart deploy/redepanda-backend
   kubectl -n redepanda rollout status deploy/redepanda-backend
   ```
   Es dürfen nie beide Pods gleichzeitig fehlen (`maxUnavailable: 0`), und im Chat darf weder eine
   Lücke noch eine Dublette entstehen.
9. **Reconnect-Anzeige.** Erst wenn das Backend *ganz* weg ist, zeigt das Frontend sein
   Ausfallverhalten:
   ```bash
   kubectl -n redepanda scale deploy/redepanda-backend --replicas=0
   ```
   Es zeigt „verbinde neu…", blendet einen Hinweis über dem Eingabefeld ein und sperrt es, bis der
   Stream wieder steht. Es versucht es dabei selbst erneut — mit wachsendem Abstand (1, 2, 4, 8,
   dann 15 Sekunden), insgesamt achtmal über gut eine Minute. Bleibt das Backend darüber hinaus weg,
   wechselt die Anzeige auf „getrennt" und der Hinweis bekommt einen Knopf „Erneut verbinden".
   Danach `--replicas=2` zurück.

### Was im Frontend absichtlich so ist

- **Verlauf ohne History-Endpunkt.** Der Consumer liest ab `AutoOffsetReset.Earliest` und baut daraus
  einen Puffer pro Raum; `GET /api/stream` schickt ihn als erste Frames, bevor der Live-Betrieb
  beginnt. Es gibt bewusst kein zweites `GET /api/history`: ein separater Aufruf hätte eine Lücke
  zwischen „Verlauf geladen" und „Stream offen" — genau dort ginge eine Nachricht verloren.
- **Kein doppelter Verlauf beim Reconnect.** Jedes Datenframe trägt seinen Kafka-Offset als
  SSE-`id`. `EventSource` schickt die zuletzt gesehene id beim automatischen Neuverbinden als
  `Last-Event-ID` mit, und das Backend spielt nur nach, was danach kam.
  Das deckt allerdings nur den Pfad ab, auf dem `EventSource` von sich aus neu verbindet. Ist der
  Fehler *fatal* — der Pod ist weg, Caddy antwortet 502 —, gibt `EventSource` endgültig auf, und das
  Frontend baut ein neues auf. An ein neues `EventSource` kann kein JavaScript einen Header hängen:
  der Server sieht einen Erstbesucher und spielt den ganzen Raum noch einmal ein. Deshalb filtert
  der Client zusätzlich selbst nach der `id` und rendert jeden Offset höchstens einmal. Beide Pfade
  zusammen ergeben die Zusage, auf der die Punkte 7 bis 9 oben beruhen.
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
| `CHAT_HISTORY_SIZE` | `200` (`0` = alles im Topic) | Backend |
| `PRODUCE_TIMEOUT_MS` | `10000` | Backend (Producer, `message.timeout.ms`) |
| `ASPNETCORE_URLS` | `http://+:8080` | Backend |
| `POD_NAME` | im Cluster Pflicht (fieldRef), lokal `MachineName-PID` | Backend (Consumer-GroupId) |
| `LOG_LEVEL` | `Information` | Backend |
| `BACKEND_HOST` | `redepanda-backend:8080` | Frontend (Caddyfile) |
| `FRONTEND_LOG_LEVEL` | `INFO` | Frontend (Caddy: Access-Log **und** Runtime-Log) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://redepanda-otel-collector:4317` | Backend |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Backend |
| `OTEL_SERVICE_NAME` | `redepanda-backend` | Backend (→ Prometheus-Label `job`) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `5000` (ms) | Backend |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.instance.id=$(POD_NAME)` | Backend (→ Label `instance`) |
| `OTEL_SDK_DISABLED` | `false` | Backend (Not-Aus für eine Demo ohne Collector) |

`FRONTEND_LOG_LEVEL` kennt `DEBUG`, `INFO`, `WARN` und `ERROR` — bewusst eine andere Schreibweise
als `LOG_LEVEL` (`Information`). Das ist Caddys Vokabular, nicht das von .NET, und Caddy weist die
.NET-Schreibweise beim Start zurück, statt still auf einen Default zurückzufallen. Die beiden
Variablen zu vereinheitlichen hieße, eine der beiden Laufzeiten anzulügen.

Alle Variablen bis einschließlich `LOG_LEVEL` liest das Backend **explizit** in `BackendOptions`
aus, statt sich auf das `Section__Key`-Autobinding von ASP.NET zu verlassen — nur so stimmen die
schlichten Namen aus dieser Tabelle mit dem Code überein.

`POD_NAME` ist dabei die einzige Variable ohne brauchbaren Default: die Consumer-GroupId wird
daraus gebildet, und zwei Replicas in *einer* Gruppe teilen sich nicht die Last, sie legen
einander still — Kafka gibt die eine Partition an genau einen Pod, alle Browser an den übrigen
sähen einen Raum, der nie mehr etwas anzeigt. Im Cluster kommt der Name aus einem `fieldRef` auf
`metadata.name` (pro Namespace garantiert eindeutig); fehlt er dort, startet der Pod **nicht**,
statt die Gruppe stillschweigend zu kollidieren. Außerhalb von Kubernetes gibt es keinen
`fieldRef`, dafür aber denselben Fehlerfall — zweimal `dotnet run` auf einer Maschine —, weshalb
der lokale Default die Prozess-ID enthält.

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
histogram_quantile(0.95, rate(http_server_request_duration_seconds_bucket[1m]))

# Eine Zeitreihe je Pod — das instance-Label ist der Pod-Name. Die Summe ist die Zahl der offenen
# Browserfenster, die Einzelwerte zeigen, wie kube-proxy sie auf die Replicas verteilt hat.
redepanda_active_connections
sum(redepanda_active_connections)
sum by (instance) (redepanda_active_connections)

# Der direkteste Beleg für den Fan-out: jede Replica konsumiert *jede* Nachricht unter eigener
# Group-ID, also ist das Verhältnis ungefähr die Replica-Zahl. Teilten sich die Pods eine Gruppe,
# stünde hier 1 — und die Hälfte der Browser sähe nichts.
sum(rate(redepanda_messages_received_total[1m])) / sum(rate(redepanda_messages_sent_total[1m]))
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
| Dependencies | NuGet zentral deklariert und per `packages.lock.json` inklusive transitiver Pakete festgenagelt, Restore nur gegen nuget.org; alle Registry-Images per Digest gepinnt; Frontend bewusst ohne Build-Tooling (Vanilla JS) | — |
| Config | ausschließlich Env-Variablen, im Cluster aus ConfigMap | — |
| Backing Services | Redpanda über `REDPANDA_BOOTSTRAP_SERVERS`, Telemetrie-Backend über `OTEL_EXPORTER_OTLP_ENDPOINT` — beide ohne Codeänderung austauschbar | — |
| Build, Release, Run | drei getrennte Stufen mit identifizierbarem Release: unveränderlicher Image-Tag + Release-Datei, `helm rollback` funktioniert (siehe unten) | keine Registry: alte Images leben nur im Image-Store der Node. Kein CI (laut Aufgabe erlaubt) |
| Processes | kein dauerhafter lokaler Zustand; SSE-Verbindungen sind bewusst prozesslokal | der Verlaufspuffer ist nur eine Projektion des Topics, die jeder Pod beim Start neu aufbaut |
| Port Binding | Backend `:8080`, Frontend `:8080`, kein externer Webserver nötig | — |
| Concurrency | **Beide** Deployments laufen mit 2 Replicas, PodDisruptionBudget, `preStop`-Drain und Rollout ohne Unterbrechung (`maxUnavailable: 0`) — der SSE-Pfad ist damit von Caddy bis Kafka redundant, nicht nur an seinem hinteren Ende. Backend zusätzlich: Consumer-GroupId pro Pod ⇒ echter Fan-out, HPA optional, kein Sticky-Session-Bedarf, weil die SSE-`id` der brokerweite Kafka-Offset ist | HPA braucht metrics-server und ist deshalb per Default aus; auf einem Ein-Node-Cluster bleiben `topologySpreadConstraints` wirkungslos |
| Disposability | SIGTERM: Consumer `Close()`, Producer `Flush()`, offene SSE-Streams enden über `ApplicationStopping` statt bis zum Timeout weiterzuheartbeaten; `preStop` 5 s + App-Timeout 25 s < Grace Period 45 s | — |
| Dev/Prod Parity | identische Images lokal und im Cluster | Redpanda läuft im `dev-container`-Modus, einzelner Broker |
| Logs | strukturiert (JSON) nach stdout, keine Logdateien: Backend über `AddJsonConsole`, Frontend als Caddy-Access-Log (eine Zeile pro Request, Probes ausgenommen) | Logs laufen bewusst **nicht** über OTLP. Die beiden JSON-Schemata sind nicht vereinheitlicht — Caddy loggt zap-artig (`ts`/`level`/`msg`/`request`), .NET mit `Timestamp`/`LogLevel`/`Category`. Ohne Aggregator kostet das nichts; mit einem wäre es das Erste, was auffällt |
| Admin Processes | Topic-Anlage als Kubernetes-Job / Helm-Hook | — |

### Build, Release, Run im Einzelnen

Die drei Stufen sind strikt getrennt, und zwischen ihnen wird nichts von Hand angefasst.

| Stufe | Wer | Ergebnis |
|---|---|---|
| **Build** | `./scripts/build-images.sh --release` | zwei Images unter `…:0.1.0-g103b98b` und `deploy/releases/0.1.0-g103b98b.yaml` |
| **Release** | `helm upgrade -f deploy/releases/<version>.yaml` | eine Helm-Revision: dieser Build + diese Konfiguration, unveränderlich |
| **Run** | kubelet | Container aus genau diesen Images |

Der Kern ist der **unveränderliche Tag**. Ein beweglicher Name wie `:dev` sieht wie ein Release
aus, ist aber keines: `helm rollback` stellt zwar die alten Werte wieder her, die zeigen dann
aber erneut auf `:dev` — also auf das *neueste* Image. Der Rollback läuft durch und ändert
nichts. Mit `0.1.0-g103b98b` zeigt die alte Revision auf ein anderes Image, und der Rollback
wirkt tatsächlich.

Daraus folgt der Rest:

- Der Tag wird **abgeleitet**, nicht getippt — `appVersion` + Commit. Ein Build lässt sich damit
  auf einen Commit zurückführen, ohne dass jemand mitschreiben muss.
- Ein Build aus unsauberem Arbeitsbaum bekommt zusätzlich einen Hash des uncommitteten Standes.
  Ein bloßes `-dirty` wäre wieder ein beweglicher Name.
- Das Chart hat **keinen Default-Tag**. Ein Default wäre notwendig beweglich, und ein
  Fehlschlag beim Rendern kostet einen Befehl — ein unidentifizierbares Image im Cluster kostet
  eine Stunde.
- Die Release-Datei wird committet und ist damit der nachvollziehbare Stand: welcher Commit,
  wann gebaut, welche Images.
- `deploy/k8s/rendered.yaml` erzeugt derselbe Lauf mit, damit der Weg ohne Helm dieselbe
  Version deployt und nicht auseinanderläuft.

Was fehlt: eine Registry und damit garantierte Aufbewahrung alter Images, und ein CI, das den
Build automatisch anstößt. Beides ist laut Aufgabe nicht gefordert; der Ausbaupfad wäre ein
`--push` in `build-images.sh` plus Digest-Pin auch für diese beiden Images.

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
REL=deploy/releases/0.1.0-g103b98b-dirty.5f4f110.yaml   # die aktuelle Release-Datei

dotnet test                                    # 64 Tests
helm lint deploy/helm/redepanda -f "$REL"
helm template redepanda deploy/helm/redepanda -n redepanda -f "$REL" \
  | kubeconform -strict -summary -kubernetes-version 1.32.0

# Der HPA ist per Default aus und wird sonst nie gerendert — also die zweite Kombination
# ausdrücklich mitprüfen, sonst validiert niemand backend-hpa.yaml:
helm lint deploy/helm/redepanda -f "$REL" --set backend.autoscaling.enabled=true
helm template redepanda deploy/helm/redepanda -n redepanda -f "$REL" \
  --set backend.autoscaling.enabled=true \
  | kubeconform -strict -summary -kubernetes-version 1.32.0

# Mit HPA darf im Deployment kein `replicas:` stehen, ohne HPA muss es dort stehen. Sonst
# überschreiben sich Helm und der Autoscaler gegenseitig, und die Pod-Zahl pendelt.
helm template redepanda deploy/helm/redepanda -f "$REL" \
  --set backend.autoscaling.enabled=true \
  --show-only templates/backend.yaml | grep -c '^  replicas:'    # erwartet: 0
helm template redepanda deploy/helm/redepanda -f "$REL" \
  --show-only templates/backend.yaml | grep '^  replicas:'       # erwartet: replicas: 2

# Ohne Release-Datei muss das Chart abbrechen — das ist der Schutz gegen ein
# unidentifizierbares Image im Cluster, also selbst prüfenswert:
helm template redepanda deploy/helm/redepanda   # erwartet: Fehler "no release selected"
# Achtung: `helm lint` fängt das nicht — Helm 4 stuft ein `fail` im Template auf INFO herab
# und meldet trotzdem "0 chart(s) failed". Nur `helm template` bricht wirklich ab.

# Reproduzierbarkeit — ohne CI muss das jemand von Hand anstoßen:
dotnet restore src/RedePanda.Backend/RedePanda.Backend.csproj --locked-mode
dotnet restore src/RedePanda.ChatClient/RedePanda.ChatClient.csproj --locked-mode
dotnet restore tests/RedePanda.Backend.Tests/RedePanda.Backend.Tests.csproj --locked-mode
./scripts/check-digests.sh
```

`deploy/k8s/rendered.yaml` ist generiert und gehört zum Release: `build-images.sh --release`
schreibt es mit. Wer `values.yaml` ändert, ohne ein Release zu schneiden, erzeugt es von Hand
neu — mit derselben Release-Datei, sonst verliert es seine Version:

```bash
helm template redepanda deploy/helm/redepanda -f "$REL" > deploy/k8s/rendered.yaml
```

> `kubectl apply --dry-run=client` eignet sich hierfür **nicht**: es kontaktiert trotz
> „client" einen API-Server, um Ressourcentypen aufzulösen, und scheitert ohne Cluster.

### Automatisiert abgedeckt

Validierung (leer/Whitespace/zu lang, Grenzwerte), Wire-Format-Roundtrip inklusive Unicode,
Ablehnung defekter Payloads, serverseitiger Zeitstempel und Raumtrennung im Broadcaster. Dazu der
Verlauf: Puffergrenzen, Raumtrennung im Puffer, Wiedergabe vor dem Live-Betrieb, `id`-Zeile auf dem
Draht und das Überspringen des bereits Gesehenen bei gesetztem `Last-Event-ID`.

Dazu die drei Zusagen, auf denen die Skalierung ruht: die Consumer-GroupId ist pro Pod eindeutig und
deterministisch aus dem Pod-Namen abgeleitet; eine Verbindung **ohne** `Last-Event-ID` bekommt den
Raum mit streng steigenden `id`s (die Voraussetzung, auf der der Client-Filter im Frontend beruht);
und ein Stream endet, sobald der Host stoppt, obwohl die Anfrage selbst nie abgebrochen wurde — der
Unterschied zwischen einem Rollout von Sekundenbruchteilen und einem von 25 Sekunden.

Nicht automatisiert abgedeckt ist der Backfill selbst (`AutoOffsetReset.Earliest`, Partition-EOF,
Readiness-Gate): die Tests laufen ohne Broker, der Consumer wird in der Fixture entfernt.

### Manuell durchzugehen

- [ ] `helm install` auf frischem Cluster → alle Pods `Ready` ohne Eingriff
- [ ] Topic-Job `Completed`
- [ ] Zwei Browserfenster, gleicher Raum → beide sehen die Nachricht
- [ ] Zwei Browserfenster, verschiedene Räume → keine Vermischung
- [ ] Konsolenclient liest dieselben Nachrichten mit
- [ ] Browser neu laden → der Verlauf des Raums ist wieder da, in richtiger Reihenfolge
- [ ] `kubectl delete pod <backend>` → nach dem Reconnect **keine** doppelten Nachrichten
- [ ] Leere Nachricht und Nachricht > `MAX_MESSAGE_LENGTH` → HTTP 400
- [ ] Frontend hat keinerlei Kafka-Zugriff (Netzwerk-Tab: nur `/api`)
- [ ] `kubectl get pods -l app.kubernetes.io/component=backend` → **zwei** Pods, beide `Ready`
- [ ] `redepanda_active_connections` steht mit **zwei** `instance`-Labels da; `sum(...)` = Zahl der
      offenen Fenster
- [ ] `sum(rate(received[1m])) / sum(rate(sent[1m]))` ≈ 2 → jede Replica bekommt jede Nachricht
- [ ] **Einen von zwei** Backend-Pods löschen → die Fenster am anderen Pod merken nichts, die am
      gelöschten setzen ohne doppelten Verlauf auf der überlebenden Replica auf
- [ ] `kubectl rollout restart deploy/redepanda-backend` während des Tippens → keine Lücke, keine
      Dublette, nie beide Pods gleichzeitig weg
- [ ] `kubectl get pdb redepanda-backend` → `ALLOWED DISRUPTIONS` = 1
- [ ] `scale --replicas=0` → Banner, Backoff und „Erneut verbinden" sind weiterhin vorführbar
- [ ] Nur mit metrics-server: `kubectl top pods -n redepanda` liefert Zahlen und die HPA-`TARGETS`
      sind keine `<unknown>`
- [ ] `kubectl -n redepanda logs deploy/redepanda-frontend` zeigt je eine JSON-Zeile pro
      Browser-Request — und **keine** für `/healthz`, obwohl beide Probes alle 10 s pollen
- [ ] Collector-Pod `Ready`, Logs ohne `permanent error` / `connection refused`
- [ ] Prometheus-Target `redepanda-otel-collector:8889` = `UP`
- [ ] `redepanda_messages_sent_total` steigt; `redepanda_active_connections` fällt beim
      Schließen eines Fensters
- [ ] Backend hat **keinen** `/metrics`-Endpunkt (`curl` → 404)
- [ ] `helm upgrade` ohne `-f deploy/releases/<version>.yaml` bricht mit der Tag-Meldung ab
- [ ] **Rollback-Probe:** zweites Release mit sichtbarer Änderung bauen und deployen →
      `helm history` zeigt beide, `helm rollback redepanda 1` → `kubectl get deploy
      redepanda-frontend -o jsonpath='{..image}'` nennt wieder den **alten** Tag und der
      Browser zeigt den alten Stand
- [ ] `helm uninstall` entfernt alles bis auf das PVC
- [ ] README von einer unbeteiligten Person nachvollzogen
- [ ] Repository public, Gruppenmitglieder eingetragen

---

## 14. Bekannte Einschränkungen

- **Der HPA ist per Default aus.** Er misst gegen `metrics.k8s.io`, und das liefert nur ein Cluster
  mit metrics-server — den weder kind noch Docker Desktop mitbringen. Angeschaltet skaliert er auf
  CPU, nicht auf `redepanda_active_connections`: das Backend hat bewusst keinen `/metrics`-Endpunkt,
  die interessantere Metrik käme also nur über prometheus-adapter oder KEDA. Ein Ausbaupfad, keine
  Lücke — aber eben auch keine Vorführung.
- **`topologySpreadConstraints` sind auf einem Ein-Node-Cluster beweisbar wirkungslos.** Sie stehen
  als `ScheduleAnyway` im Chart, weil eine harte Constraint die zweite Replica dort für immer
  `Pending` ließe. Auf einem Node ist das dokumentierter Code, kein getestetes Verhalten — und ein
  `kubectl drain` nimmt dort ohnehin beide Pods mit, PodDisruptionBudget hin oder her.
- **Der Verlauf liegt im Speicher jedes Pods**, und damit `replicas`-mal im Cluster. Jeder Pod liest
  den Topic beim Start von vorn und hält das Ergebnis pro Raum vor; `CHAT_HISTORY_SIZE` begrenzt das
  inzwischen auf 200 Nachrichten **pro Raum**. Drei ehrliche
  Reste: die Zahl der Räume bleibt unbegrenzt, begrenzt wird der Speicher und nicht die Startzeit
  (der Consumer liest den Topic weiterhin ganz), und wer länger als 200 Nachrichten weg war, bekommt
  beim Wiedereinstieg eine **Lücke** statt einer Dublette. Mit `0` bleibt das alte Verhalten
  erreichbar.
- **Der Pod wird erst `Ready`, wenn der Verlauf geladen ist.** Richtig so — sonst bekäme der erste
  Besucher nach einem Rollout eine halbe Historie —, aber es koppelt die Readiness an den Broker:
  existiert der Topic noch nicht, bleibt der Pod so lange `NotReady`, bis der Topic-Job durch ist.
- **Redpanda im `dev-container`-Modus**, ein einzelner Broker ohne Replikation. Für eine Demo
  richtig, für Produktion nicht.
- **Der Broker-Pod läuft ohne `readOnlyRootFilesystem`**, als einziger im Chart:
  `rpk redpanda start` schreibt bei jedem Start die zusammengeführte Konfiguration nach
  `/etc/redpanda/redpanda.yaml`.
- **Kein Ingress, keine Authentifizierung, kein TLS.** Port-Forward genügt für die Demo.
- **Kein CI.** Die Reproduzierbarkeits-Prüfungen aus Abschnitt 13 — `--locked-mode` und
  `./scripts/check-digests.sh` — laufen deshalb nicht automatisch. Sie melden Drift nur, wenn
  jemand sie aufruft.

### Was in dieser Session *nicht* auf einem echten Cluster verifiziert wurde

Ehrlichkeitshalber: auf dem Entwicklungsrechner war kein Cluster verfügbar (rootless Podman
ohne `cpuset`-Delegation, kein kubectl-Kontext). Verifiziert wurden Build, Tests, beide Images,
der komplette Chat gegen ein echtes Redpanda, der komplette Metrikpfad gegen einen echten
OTel-Collector sowie `helm lint`, `helm template` und `kubeconform --strict` für alle drei
Wertekombinationen.

**Nicht** verifiziert und beim ersten echten Deployment zuerst zu prüfen: Pods erreichen
`Ready`, der Topic-Job läuft durch, Prometheus-Target `UP`, `honor_labels`-Verhalten,
Pod-Neustart-Resilienz und dass `helm uninstall` das PVC stehen lässt.

Für die Nebenläufigkeit gilt dasselbe: dass zwei Replicas einander vertreten, folgt aus dem
Offset-basierten Wiedereinstieg und ist auf Test- und Wire-Ebene abgesichert — die Demo-Punkte 7 bis
9 aus Abschnitt 8 und die neuen Punkte der Abnahmeliste sind aber noch von Hand nachzuvollziehen.

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

**Access-Logs des Frontends prüfen — ohne Cluster und ohne Build.** `caddy validate` beantwortet
die Frage *nicht*: eine Konfiguration ganz ohne Access-Log ist gültig. `caddy adapt` zeigt dagegen,
was der Server tatsächlich bekommt:

```bash
podman run --rm -v "$PWD/src/RedePanda.Frontend/Caddyfile:/Caddyfile:ro" \
  docker.io/library/caddy:2.11.4-alpine caddy adapt --config /Caddyfile --adapter caddyfile
```

Unter `apps.http.servers.srv0` muss ein `logs`-Objekt stehen (`{"default_logger_name":"log0"}`).
Fehlt es, loggt der Server keinen einzigen Request — egal was im globalen Block steht.

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
| Frontend-Logs zeigen keine Requests | Die `log`-Direktive steht im **globalen** Block. Dort konfiguriert sie Caddys Runtime-Logger und erzeugt keine einzige Request-Zeile. Access-Logs gibt es nur mit `log` **im Site-Block** — und `caddy validate` meldet die fehlende Zeile nicht |

---

## Projektstruktur

```text
src/RedePanda.Contracts/    ChatMessage + Validierung + Wire-Format (geteilt)
src/RedePanda.Backend/      ASP.NET Core: SSE, Kafka, OpenTelemetry
src/RedePanda.Frontend/     Caddyfile + Vanilla-JS-Frontend (index.html, style.css, app.js, favicon.svg)
src/RedePanda.ChatClient/   Konsolenclient
tests/                      xUnit
deploy/helm/redepanda/      Helm-Chart
deploy/releases/            generierte Release-Dateien (Image-Tags + Commit pro Build)
deploy/k8s/rendered.yaml    generiert via `helm template` aus der Release-Datei
scripts/                    build-images.sh, check-digests.sh, demo.sh
RedePanda-kafka-docker/     Redpanda für lokale Entwicklung ohne Kubernetes
```
