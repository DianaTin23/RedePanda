# RedeTim

Ein Browser-Chat, bei dem **Frontend** und **Backend** als zwei getrennte, selbst geschriebene
Deployments in Kubernetes laufen und ausschließlich über **Redpanda** (Kafka-Protokoll)
miteinander sprechen — installiert per **Helm** auf einem lokalen **kind**-Cluster.

RedeTim ist die Weiterentwicklung von RedePanda, einer Konsolenanwendung aus einer Vorlesung
über verteilte Systeme, die sich nicht aus dem Browser bedienen ließ.

- **Backend:** ASP.NET Core Minimal API ([`src/RedeTim.Backend/`](./src/RedeTim.Backend/)) —
  nimmt Nachrichten per HTTPS an, schreibt sie nach Redpanda und liefert sie als
  Server-Sent-Events-Stream wieder aus.
- **Frontend:** Vanilla-JS-Oberfläche hinter Caddy
  ([`src/RedeTim.Frontend/`](./src/RedeTim.Frontend/)) — kein npm, kein CDN, keine Webfonts.
- **Orchestrierung:** ein Helm-Chart für den gesamten Stack
  ([`deploy/helm/redetim/`](./deploy/helm/redetim/)) inklusive Broker, selbst ausgestellter
  TLS-Zertifikate und Traefik-Ingress.

![RedeTim](src/RedeTim.Frontend/wwwroot/login-background-dark.png)

---

## Gruppenmitglieder

| Name | GitHub |
|---|---|
| Manuel Schülein | [@deadmade](https://github.com/deadmade) |
| Diana Huynh | [@DianaTin23](https://github.com/DianaTin23) |
| Mara Küfer | [@maratin23](https://github.com/maratin23) |

---

## Architektur

```text
Browser ──HTTPS──▶ Traefik ──▶ Caddy (Frontend-Pod) ──proxy /api──▶ Backend-Pod ──Kafka──▶ Redpanda
   ▲                 :8443       :8443                 HTTPS :8443     │                (StatefulSet)
   └──────────────── SSE-Stream (/api/stream) ◀────────────────────────┘
```

Das Frontend kennt Kafka nicht: es spricht ausschließlich `/api/...`, und im Netzwerk-Tab des
Browsers lässt sich das nachweisen. Der Verlauf eines Raums kommt nicht aus einem eigenen
History-Endpunkt, sondern als erste Frames desselben SSE-Streams — die `id` jedes Frames ist der
Kafka-Offset, weshalb ein Reconnect auf einer beliebigen Backend-Replica lückenlos aufsetzt.

Jede HTTP-Strecke im Release ist TLS, und jeder Client prüft das Zertifikat seines Gegenübers
gegen die CA, die das Chart bei der ersten Installation selbst ausstellt — `insecure_skip_verify`
steht an keiner Stelle.

---

## Setup

Lokales Deployment auf einem **kind**-Cluster via **Helm**, inklusive Traefik-Ingress.

### Voraussetzungen

* Docker oder Podman
* Kind
* Kubectl
* Helm 4 (entwickelt gegen 4.2.3)

Wer **Nix** benutzt, bekommt alles über die mitgelieferte Dev-Shell — dazu .NET 10, `rpk`,
`kubeconform` und `skopeo`:

```bash
nix develop        # oder: direnv allow
```

Die Images liegen fertig in `ghcr.io` und werden gezogen, nicht gebaut. Ein .NET SDK braucht nur,
wer selbst baut.

---

### Setup & Deployment Schritt für Schritt

#### 1. Kind-Cluster mit Port-Mapping erstellen

```bash
kind create cluster --config kind-config.yaml
```

*(Verwendet [`kind-config.yaml`](./kind-config.yaml) — mappt 8443 und 8080 auf den Host. Die
Ports lassen sich nur beim Anlegen setzen, deshalb ist es nicht irgendein Cluster.)*

---

#### 2. Helm-Dependencies herunterladen

Lädt das Sub-Chart des Traefik Ingress Controllers nach `charts/`, versionsgenau nach
[`Chart.lock`](./deploy/helm/redetim/Chart.lock):

```bash
helm repo add traefik https://traefik.github.io/charts
helm dependency build deploy/helm/redetim
```

---

#### 3. Release wählen und deployen

```bash
REL=$(./scripts/select-release.sh)

helm upgrade --install redetim ./deploy/helm/redetim \
  -n redetim --create-namespace --wait --timeout 10m \
  -f "$REL" --description "release $(basename "$REL" .yaml)"
```

Die Release-Datei aus [`deploy/releases/`](./deploy/releases/) ist **Pflicht**, nicht optional:
sie pinnt den unveränderlichen Image-Tag. Ohne sie bricht das Chart beim Rendern mit einer klaren
Meldung ab, statt irgendein Image zu starten — und genau das macht ein späteres `helm rollback`
wirksam.

---

#### 4. Status überprüfen

Warten, bis alle Pods `Running`/`Ready` sind und der Topic-Job `Completed` ist:

```bash
kubectl -n redetim get pods
```

---

### Anwendung aufrufen

Sobald alle Pods laufen, ist die Anwendung im Browser erreichbar:

**https://localhost:8443**

Die CA gehört diesem Release, kein Browser kennt sie — beim ersten Aufruf kommt deshalb eine
Zertifikatswarnung, die einmal je Port zu akzeptieren ist. Wer sie nicht sehen will, importiert
die CA in den eigenen Truststore:

```bash
kubectl -n redetim get secret redetim-ca \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/redetim-ca.crt
```

`http://localhost:8080` landet auf der TLS-Adresse, statt in einem Verbindungsfehler — per `301`
auf `GET`, per `308` bei allem anderen, damit Methode und Body erhalten bleiben.

---

### Aufräumen

* **Helm-Release löschen:**
  ```bash
  helm uninstall redetim -n redetim
  ```
* **Kind-Cluster komplett entfernen:**
  ```bash
  kind delete cluster
  ```

Das PVC des Brokers bleibt beim `helm uninstall` absichtlich stehen — PVCs aus
`volumeClaimTemplates` gehören dem StatefulSet-Controller, nicht dem Helm-Release. Wer wirklich
bei null anfangen will: `kubectl -n redetim delete pvc --all`.

---

## Images bauen

> **Zum Ausprobieren nicht nötig.** Die drei Images liegen in `ghcr.io`, der Cluster zieht sie
> selbst. Dieser Abschnitt beschreibt, wie sie dorthin kommen.

```bash
./scripts/build-images.sh            # nur bauen
./scripts/build-images.sh --release  # Release schneiden (nur aus sauberem Baum)
./scripts/build-images.sh --push     # --release, dann nach ghcr.io schieben
```

Der Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus
[`Chart.yaml`](./deploy/helm/redetim/Chart.yaml) plus der kurze Git-Commit, also
`ghcr.io/dianatin23/redetim-backend:0.1.0-g103b98b`. Er wird nie wiederverwendet. Am Ende
schreibt das Skript die dazugehörige Release-Datei nach `deploy/releases/` und gibt den
Deploy-Befehl aus.

GHCR legt ein Paket beim ersten Push **privat** an, unabhängig davon, ob das Repository public
ist — solange das so bleibt, scheitert jeder Pull im Cluster mit `ImagePullBackOff`. Einmalig
durch den Repository-Eigentümer je Paket auf *Public* stellen; alternativ ein Pull-Secret
anlegen und über `imagePullSecrets` setzen.

---

## Konfiguration

Alle laufzeitvariablen Parameter kommen aus **Umgebungsvariablen** unter schlichten Namen, die
[`BackendOptions`](./src/RedeTim.Backend/BackendOptions.cs) explizit ausliest. Im Cluster stammen
die nicht geheimen Werte aus einer [ConfigMap](./deploy/helm/redetim/templates/configmap.yaml),
`POD_NAME` kommt über die Downward API und TLS-Zertifikate werden als Secret-Dateien gemountet.

Zugangsdaten stehen **nie** in der ConfigMap oder in `values.yaml`, sondern kommen per
`secretKeyRef` aus einem Secret, das man selbst anlegt (`redpanda.auth.existingSecret`).

Sämtliche Schalter stehen kommentiert in
[`values.yaml`](./deploy/helm/redetim/values.yaml) — Replicas, Autoscaling, Topic-Namen,
Verlaufsgrenzen, Ingress-Host und die Broker-Anbindung.

---

## Eingesetzte Technologien aus der CNCF-Landscape

**Helm** (*graduated*) ist der einzige Installationsweg. Das Chart bündelt alle Komponenten —
Backend, Frontend, Broker-StatefulSet, ConfigMap, TLS-Secrets, Topic-Job und Ingress — und bindet
Traefik als Sub-Chart über [`Chart.yaml`](./deploy/helm/redetim/Chart.yaml) ein. Sinnvoll ist das
hier aus drei Gründen: der gesamte Stack wird mit **einem** Befehl deterministisch ausgerollt und
sauber wieder entfernt; umgebungsspezifische Werte sind in
[`values.yaml`](./deploy/helm/redetim/values.yaml) parametrisiert, statt Manifeste zu duplizieren;
und jedes Deployment ist eine **Revision**, die sich mit `helm rollback` zurückholen lässt — was
nur trägt, weil jede Revision einen unveränderlichen Image-Tag mitbringt.

**Kubernetes** (*graduated*) ist die Laufzeitplattform.

**Traefik** ist der Ingress Controller und kommt als versionsgenaues Sub-Chart mit.
`ingress-nginx` wäre die naheliegende Wahl gewesen, ist aber im März 2026 zurückgezogen worden
und bekommt keine Sicherheitsupdates mehr.

**Redpanda** ist der Broker, über den die beiden Anwendungen ausschließlich miteinander reden. In
der CNCF-Landscape gelistet, aber **kein** CNCF-gehostetes Projekt und nicht Open Source im
engeren Sinne (BSL 1.1, Apache-2.0 nach vier Jahren).

---

## 12 Faktoren

Die zwölf Faktoren von [12factor.net](https://12factor.net/) und ihre Umsetzung in diesem Repo.

### I Codebase
> *"One codebase tracked in revision control, many deploys."*

* Ein Git-Repository, vier .NET-Projekte und **eine** Beschreibung des Deployments: das Chart.
  Ein zweites, gerendertes Manifest lag hier einmal daneben und lief zuverlässig auseinander.
* Dieselbe Codebasis rollt lokal auf kind und gegen einen fremden Broker aus — der Unterschied
  sind Werte, nicht Code.

### II Dependencies
> *"Explicitly declare and isolate dependencies."*

* Alle NuGet-Versionen zentral in
  [`Directory.Packages.props`](./Directory.Packages.props); eine `Version` in einer `.csproj`
  bricht den Restore absichtlich.
* `packages.lock.json` je Projekt pinnt den vollständigen Graphen **inklusive transitiver**
  Pakete mit Content-Hashes; der Container-Build restauriert mit `--locked-mode`, sodass eine
  veraltete Lock-Datei ein Build-Fehler wird statt eines stillen Upgrades.
* Jedes Registry-Image ist per **Digest** gepinnt, das Traefik-Sub-Chart per
  [`Chart.lock`](./deploy/helm/redetim/Chart.lock), das SDK per
  [`global.json`](./global.json).
* Das Frontend hat bewusst kein Build-Tooling: statische Dateien, die Caddy ausliefert.
* Probe: [`./scripts/check-repro.sh`](./scripts/check-repro.sh).

### III Config
> *"Store config in the environment."*

* [`BackendOptions.FromEnvironment()`](./src/RedeTim.Backend/BackendOptions.cs) liest jede
  Variable **explizit** aus, statt sich auf Autobinding zu verlassen.
* Im Cluster kommen die nicht geheimen Werte aus der
  [ConfigMap](./deploy/helm/redetim/templates/configmap.yaml), `POD_NAME` über die Downward API,
  Zertifikate als Secret-Mount.
* Zugangsdaten nie in `values.yaml`, immer per `secretKeyRef` aus einem eigenen Secret.

### IV Backing Services
> *"Treat backing services as attached resources."*

* Redpanda hängt an `REDPANDA_BOOTSTRAP_SERVERS` und ist ohne Codeänderung austauschbar — **auch
  im Chart**: `redpanda.enabled=false` plus `redpanda.external.bootstrapServers`.
* [`KafkaSecurity`](./src/RedeTim.Contracts/KafkaSecurity.cs) konfiguriert TLS und SASL/SCRAM für
  *jeden* Kafka-Client im Repo — Producer, Consumer und Admin.
* Fehlt bei `redpanda.enabled=false` die Adresse oder bei einem SASL-Protokoll das Secret, bricht
  das Chart beim Rendern ab, statt einen Pod zu starten, der jede Verbindung scheitern lässt.

### V Build, Release, Run
> *"Strictly separate build and run stages."*

* **Build:** [`./scripts/build-images.sh --release`](./scripts/build-images.sh) baut die drei
  Images unter einem abgeleiteten, unveränderlichen Tag und schreibt die Release-Datei.
* **Release:** `helm upgrade -f deploy/releases/<version>.yaml` = dieser Build plus diese
  Konfiguration, als Helm-Revision festgehalten.
* **Run:** das kubelet startet Container aus genau diesen Images.
* Das Chart hat **keinen Default-Tag**. Ein beweglicher Name wie `:dev` sähe wie ein Release aus,
  ließe aber jedes `helm rollback` wirkungslos durchlaufen.

### VI Processes
> *"Execute the app as one or more stateless processes."*

* Kein dauerhafter lokaler Zustand: der Verlauf in
  [`ChatHistory`](./src/RedeTim.Backend/ChatHistory.cs) ist nur eine Projektion des Topics, die
  jeder Pod beim Start neu aufbaut.
* Die Wahrheit liegt im Broker — deshalb überlebt der Chat den Ausfall eines Pods, und deshalb
  braucht es weder Sticky Sessions noch einen Backplane.

### VII Port Binding
> *"Export services via port binding."*

* Das Backend bringt seinen HTTP-Server selbst mit (Kestrel, `:8443`), das Frontend ebenso
  (Caddy, `:8443` plus `:8080` nur für die Weiterleitung auf die TLS-Adresse).
* **TLS terminiert der Prozess selbst** — kein Sidecar und kein Terminator, den das Deployment
  mitbringen müsste. Routing und Lastverteilung liegen außerhalb der Anwendung, bei
  Kubernetes-Services und dem [Ingress](./deploy/helm/redetim/templates/ingress.yaml): Traefik
  terminiert dort das Browser-TLS und verschlüsselt zum Frontend-Pod neu, statt dessen Port im
  Klartext zu erwarten.

### VIII Concurrency
> *"Scale out via the process model."*

* **Beide** Deployments laufen mit zwei Replicas, einem PodDisruptionBudget
  (`maxUnavailable: 1`) und einem Rolling Update mit `maxUnavailable: 0`
  ([`backend.yaml`](./deploy/helm/redetim/templates/backend.yaml),
  [`frontend.yaml`](./deploy/helm/redetim/templates/frontend.yaml)) — der SSE-Pfad ist damit von
  Caddy bis Kafka redundant.
* **Eine Consumer-Gruppe pro Pod** (`redetim-backend-<POD_NAME>`) ⇒ echter Fan-out statt
  Lastverteilung: jede Replica sieht jede Nachricht.
* Die SSE-`id` ist der Kafka-Offset und gilt damit brokerweit, nicht pro Pod — ein Reconnect
  landet auf einer beliebigen Replica und liest ohne Lücke und ohne Dublette weiter.
* Optionaler HPA auf CPU-Basis
  ([`backend-hpa.yaml`](./deploy/helm/redetim/templates/backend-hpa.yaml)), per Default aus, weil
  er metrics-server braucht.

### IX Disposability
> *"Maximize robustness with fast startup and graceful shutdown."*

* SIGTERM: `preStop`-Drain, Consumer `Close()`, Producer `Flush()`, und offene SSE-Streams enden
  über `ApplicationStopping`, statt bis zum Timeout weiterzuheartbeaten.
* Caddy bekommt `grace_period 5s` — sonst wartete es unbegrenzt auf SSE-Antworten, die per
  Definition nie fertig werden.
* Readiness hängt am Broker: ein Pod wird erst `Ready`, wenn sein Verlauf geladen ist.

### X Dev/Prod Parity
> *"Keep development, staging, and production as similar as possible."*

* Derselbe Broker lokal und im Cluster, **inklusive Digest** und im selben `--mode=dev-container`
  ([`docker-compose.yml`](./RedeTim-kafka-docker/docker-compose.yml),
  [`values.yaml`](./deploy/helm/redetim/values.yaml)).
* Dasselbe .NET-SDK-Feature-Band in [`global.json`](./global.json), [`flake.nix`](./flake.nix) und
  beiden Build-Dockerfiles.

### XI Logs
> *"Treat logs as event streams."*

* Strukturiert (JSON) nach stdout, keine Logdateien: das Backend über `AddJsonConsole`, das
  Frontend als Caddy-Access-Log mit einer Zeile pro Request.
* Auch librdkafkas eigene Ausgabe geht über `SetLogHandler`
  ([`KafkaLogging.cs`](./src/RedeTim.Backend/KafkaLogging.cs)) durch `ILogger`, statt roh und an
  `LOG_LEVEL` vorbei auf stderr.

### XII Admin Processes
> *"Run admin/management tasks as one-off processes."*

* `--ensure-topic` läuft aus **demselben Build unter demselben Tag** wie die Anwendung
  ([`src/RedeTim.ChatClient/`](./src/RedeTim.ChatClient/)) und mit **derselben ConfigMap** per
  `envFrom` — bei jedem Install und Upgrade als
  [Job](./deploy/helm/redetim/templates/topic-job.yaml).
* Kein Shell-Skript in einem fremden Image, keine zweite Konfigurationsquelle.

---

## Tests

```bash
dotnet test -p:ContinuousIntegrationBuild=true  # gesamte Suite, locked mode wie in CI
./scripts/validate-chart.sh                     # beide HPA-Varianten, Ingress, Negativfall
./scripts/check-repro.sh                        # alle vier Projekte gegen ihre Lock-Dateien
./scripts/check-digests.sh                      # Image-Digests + Broker-Parität lokal/Cluster
```

`validate-chart.sh` ist die einzige Stelle, an der die Chart-Regeln stehen; CI ruft dasselbe
Skript auf. Es rendert **dreimal** — ohne HPA, mit HPA und ohne Ingress —, weil beide Schalter
per Vorgabe auf einer Seite stehen und die andere sonst nie jemand validiert, prüft die Kopplung
zwischen `replicas` und HPA, die Ingress-Verdrahtung, und dass ein Rendern **ohne** Release-Datei
abbricht. `helm lint` genügt dafür nicht — Helm 4 stuft
ein `fail` im Template auf INFO herab.

Je Anliegen ein Workflow unter [`.github/workflows/`](./.github/workflows/): `dotnet.yml` und
`chart.yml` bei jedem Push und PR, `release.yml` nur von Hand, `digests.yml` wöchentlich. Einen
Cluster hat CI nicht.

---

## Projektstruktur

```text
src/RedeTim.Contracts/    ChatMessage + Validierung + Wire-Format + KafkaSecurity (geteilt)
src/RedeTim.Backend/      ASP.NET Core: SSE, Kafka
src/RedeTim.Frontend/     Caddyfile + Vanilla-JS-Frontend (index.html, style.css, app.js)
src/RedeTim.ChatClient/   Konsolenclient und Admin-Prozess (--ensure-topic)
tests/                    xUnit
deploy/helm/redetim/      Helm-Chart
deploy/releases/          generierte Release-Dateien (Image-Tags + Commit pro Build)
scripts/                  build-images.sh, validate-chart.sh, select-release.sh,
                          check-repro.sh, check-digests.sh, lib/common.sh
kind-config.yaml          lokaler Cluster mit den Port-Mappings 8443 und 8080
RedeTim-kafka-docker/     Redpanda für lokale Entwicklung ohne Kubernetes
```
