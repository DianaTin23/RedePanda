# RedeTim

Ein Browser-Chat, bei dem **Frontend** und **Backend** als zwei getrennte, selbst geschriebene
Deployments in Kubernetes laufen und über **Redpanda** (Kafka-Protokoll) miteinander sprechen —
installiert per **Helm**.
Die Anwendung ist eine Weiterentwicklung des Projekts RedePanda, das im Rahmen einer verteilten 
Systeme Vorlesung entwickelt wurde. Bei RedePanda handelte es sich um eine sehr einfache 
Konsolenanwendung, die nicht aus dem Browser aufgerufen werden konnte.

![RedeTim](src/RedeTim.Frontend/wwwroot/login-background-dark.png)

> **Diese README beschreibt, wie man RedeTim baut, installiert und bedient.**
> Warum es so gebaut ist — die Entwurfsentscheidungen und die Fehler, die dahinterstehen —
> steht in **[docs/](docs/README.md)**.

---

## 1. Gruppenmitglieder

| Name | GitHub |
|---|---|
| Manuel Schülein | [@deadmade](https://github.com/deadmade) |
| Diana Huynh | [@DianaTin23](https://github.com/DianaTin23) |
| Mara Küfer | [@maratin23](https://github.com/maratin23) |

---

## 2. Projektziel und Architektur

Zwei eigene Anwendungen: Das Browser-Frontend kommuniziert per HTTPS und SSE mit dem Backend;
das Backend veröffentlicht und empfängt die Chatnachrichten über Redpanda. Das Frontend kennt
Kafka nicht — das ist Absicht und lässt sich in der Demo nachweisen.

```text
Browser ──HTTPS──▶ Caddy (Frontend-Pod) ──proxy /api──▶ Backend-Pod ──Kafka──▶ Redpanda
   ▲                 :8443                 HTTPS :8443     │                  (StatefulSet)
   └────────── SSE-Stream (/api/stream) ◀──────────────────┘
```

**Jede HTTP-Strecke im Release ist TLS**, und jeder Client *prüft* das Zertifikat seines
Gegenübers gegen die CA, die das Chart bei der ersten Installation selbst ausstellt —
`insecure_skip_verify` steht an keiner Stelle. Wie das eingerichtet ist und wie man der CA
vertraut, steht in Abschnitt 7 unter „Zertifikate". Eine Ausnahme gibt es, bewusst und in
Abschnitt 13 aufgeführt.

---

## 3. Kommunikationswege

| Strecke | Protokoll | Details |
|---|---|---|
| Browser → Frontend | **HTTPS** | statische Dateien, `:8443`; `:8080` antwortet nur mit `308` auf die TLS-Adresse |
| Browser → Backend | **HTTPS** über den Caddy-Proxy | alles unter `/api` |
| Frontend → Backend | **HTTPS** `:8443` | Caddy prüft das Backend-Zertifikat gegen die Release-CA |
| Backend → Browser | **SSE** (`text/event-stream`) | `GET /api/stream?room=X`, Verlauf zuerst, Heartbeat alle 15 s |
| Backend → Redpanda | Kafka | `ProduceAsync`, Key = Raumname. Plaintext beim mitgelieferten Broker, TLS/SASL konfigurierbar — Abschnitt 7, „Gegen einen fremden Broker deployen" |
| Redpanda → Backend | Kafka | Consumer-Gruppe pro Pod, `AutoOffsetReset.Earliest` (Verlauf) |

**Warum SSE und nicht SignalR**, warum es auch bei mehreren Replicas weder Sticky Sessions noch
ein Backplane braucht, und warum ein einziges Topic für alle Räume genügt:
siehe [docs/architecture.md](docs/architecture.md) und [docs/streaming.md](docs/streaming.md).

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

Die committeten `packages.lock.json` pinnen den vollständigen Graphen inklusive der transitiven
Pakete. Zwei Regeln im Alltag:

- **Version geändert?** Ein normales `dotnet restore` schreibt die Lockfile neu. Das Ergebnis
  gehört mit in den Commit.
- **Container-Build:** `dotnet restore --locked-mode` — eine nicht mehr passende Lockfile wird
  zum Build-Fehler (`NU1004`) statt zu einem stillen Upgrade.

`global.json` ist auf **10.0.302** gepinnt, dasselbe Feature-Band wie die Nix-Shell und das
SDK-Image im Backend-`Dockerfile`. Jedes Image aus einer Registry ist zusätzlich per **Digest**
gepinnt, nicht nur per Tag.

```bash
./scripts/check-repro.sh      # prueft alle vier Projekte gegen ihre Lockfiles
./scripts/check-digests.sh    # meldet, wenn ein Tag von seinem Digest weggewandert ist
```

Warum locked mode nicht dauerhaft an ist, warum der Manifest-Listen-Digest gepinnt wird und
warum die selbst gebauten Images keinen Digest tragen:
siehe [docs/build.md](docs/build.md).

**Zum Ausprobieren braucht es davon nur einen Cluster, `kubectl` und Helm.** Die Images liegen
in `ghcr.io` und werden gezogen, nicht gebaut; .NET SDK und Container-Engine braucht nur, wer
selbst baut (Abschnitt 6). Sonst direkt weiter mit Abschnitt 7.

---

## 5. Lokal ohne Kubernetes

Der schnellste Weg, den Chat zu sehen — nur Redpanda im Container, alles andere direkt.

```bash
# 1. Broker starten
cd RedeTim-kafka-docker
docker compose --env-file env.local up -d

# 2. Topic anlegen -- derselbe Admin-Prozess, den im Cluster der Topic-Job ausführt.
#    (`rpk topic create redetim-chat -p 1 -r 1 -X brokers=127.0.0.1:19092` tut dasselbe.)
REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
dotnet run --project src/RedeTim.ChatClient -- --ensure-topic

# 3. Backend starten
REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
ASPNETCORE_URLS=http://127.0.0.1:5080 \
dotnet run --project src/RedeTim.Backend

# 4. Konsolenclient in einem zweiten Terminal
REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
dotnet run --project src/RedeTim.ChatClient -- --nick alice --room general
```

Eine Nachricht über das Backend schicken und im Client mitlesen:

```bash
curl -X POST http://127.0.0.1:5080/api/messages \
  -H 'content-type: application/json' \
  -d '{"room":"general","nickname":"web","text":"hallo"}'
```

Der Konsolenclient liest **absichtlich alle Räume** mit. Das ist der einfachste Beleg dafür,
dass Backend und Client wirklich dasselbe Kafka-Topic benutzen.

**Warum hier `http://` steht und im Cluster nicht.** Das Zertifikat ist Konfiguration des
Deployments, nicht Inhalt des Images: es kommt aus einem Secret, das erst beim Installieren
entsteht (Abschnitt 7, „Zertifikate"). Lokal gibt es kein Secret und keine CA, also gibt es auch nichts zu
prüfen — ein selbst gebasteltes Zertifikat für einen Prozess auf `127.0.0.1` würde nur den
Klick auf „Warnung akzeptieren" hinzufügen, ohne irgendetwas zu sichern. Wer die TLS-Variante
lokal sehen will, gibt Kestrel dieselben zwei Variablen wie das Chart:

```bash
ASPNETCORE_URLS=https://127.0.0.1:5443 \
ASPNETCORE_Kestrel__Certificates__Default__Path=/pfad/tls.crt \
ASPNETCORE_Kestrel__Certificates__Default__KeyPath=/pfad/tls.key \
dotnet run --project src/RedeTim.Backend
```

### Gegen einen abgesicherten Broker (TLS + SASL/SCRAM)

Der Broker oben spricht auf `19092` Plaintext. Derselbe Container bringt auf `19093` einen zweiten
Listener mit, der das nicht tut: TLS plus SASL/SCRAM. Er entsteht, sobald `tls/`
Schlüsselmaterial enthält — vorher startet der Broker unverändert im reinen Plaintext-Betrieb.

Wer die Behauptung aus Abschnitt 10 prüfen will — dass der Broker ein austauschbarer Backing
Service ist —, ändert also **nichts am Broker**, sondern nur die Konfiguration der Clients:

```bash
cd RedeTim-kafka-docker
./make-tls.sh                                          # einmalig: CA + Broker-Zertifikat nach tls/
docker compose --env-file env.local up -d --force-recreate
cd ..

export REDPANDA_BOOTSTRAP_SERVERS=127.0.0.1:19093
export REDPANDA_SECURITY_PROTOCOL=SaslSsl
export REDPANDA_SASL_MECHANISM=ScramSha512
export REDPANDA_SASL_USERNAME=chat
export REDPANDA_SASL_PASSWORD=chat-secret-pw
export REDPANDA_SSL_CA_LOCATION=$PWD/RedeTim-kafka-docker/tls/ca.crt

dotnet run --project src/RedeTim.ChatClient -- --ensure-topic     # legt den Topic an -- und ist
                                                                  # zugleich die Probe, dass die
                                                                  # SASL/TLS-Einstellungen ankommen
ASPNETCORE_URLS=http://127.0.0.1:5080 \
  dotnet run --project src/RedeTim.Backend
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:5080/health/ready   # 200
```

Diese `200` ist der Punkt der Übung. Bis vor Kurzem war sie eine `503` — und zwar dauerhaft, gegen
jeden abgesicherten Broker: `BrokerReadiness` baute als einziger Kafka-Client im Repo
seinen Admin-Client ohne die Sicherheitseinstellungen. Producer und Consumer verbanden sich normal,
der Consumer las den Topic vollständig, und nur die Readiness-Prüfung sprach Plaintext gegen einen
TLS-Listener. Der Grund dafür wurde auf `Debug` verschluckt, während das Chart auf `Information`
läuft, und mit `maxUnavailable: 0` wurde daraus ein Rollout, der nie fertig wird. Von außen sah das
exakt wie ein defekter Broker aus.

Das Schlüsselmaterial in `tls/` ist gitignored und wertlos — `make-tls.sh` erzeugt es jederzeit neu.
Das Passwort steht im Klartext in der Compose-Datei, weil dieser Broker nichts schützt.

---

## 6. Images bauen und nach ghcr.io schieben

> **Zum Ausprobieren nicht nötig.** Die drei Images liegen fertig in `ghcr.io`, der Cluster zieht
> sie selbst — weiter mit Abschnitt 7. Dieser Abschnitt beschreibt, wie sie dorthin kommen.

```bash
./scripts/build-images.sh            # nur bauen
./scripts/build-images.sh --release  # Release schneiden (nur aus sauberem Baum)
./scripts/build-images.sh --push     # --release, dann nach ghcr.io schieben
```

Der Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus `Chart.yaml` plus der kurze
Git-Commit, also `ghcr.io/dianatin23/redetim-backend:0.1.0-g103b98b`. Den Namensteil davor liest
das Skript aus `values.yaml`, damit es nichts unter einem Namen baut, den das Chart nicht
ausrollt. Er wird nie wiederverwendet. Am Ende schreibt das Skript die dazugehörige
**Release-Datei** und gibt den Deploy-Befehl aus:

```text
==> Built redetim-backend:0.1.0-g103b98b, redetim-chatclient:0.1.0-g103b98b and redetim-frontend:0.1.0-g103b98b
==> Wrote deploy/releases/0.1.0-g103b98b.yaml

Deploy this release:

  helm upgrade --install redetim deploy/helm/redetim \
    -n redetim --create-namespace --wait --timeout 10m \
    -f deploy/releases/0.1.0-g103b98b.yaml \
    --description "release 0.1.0-g103b98b"
```

Bei einem **unsauberen Arbeitsbaum** warnt das Skript und hängt einen Hash des uncommitteten
Standes an (`0.1.0-g103b98b-dirty.5f4f110`) — auch dieser Tag bleibt damit eindeutig, aber die
Datei ist per `.gitignore` von einem Release ausgenommen. `--release` bricht in dem Fall ab.

### GHCR-Pakete müssen einmal auf public gestellt werden

Das ist die einzige Voraussetzung dafür, dass eine Installation **ohne eigenen Build** gelingt:
GHCR legt ein Paket beim ersten Push **privat** an, unabhängig davon, ob das Repository public
ist. Solange das so bleibt, scheitert jeder Pull im Cluster mit `ImagePullBackOff` — einem
Fehler, der wie ein Tippfehler im Image-Namen aussieht, aber keiner ist.

Einmalig, durch den Eigentümer des Repositories: `github.com/users/dianatin23/packages` → je
Paket *Package settings* → *Change visibility* → **Public**, und *Connect repository*. Danach
zieht `docker pull` auch abgemeldet — und der Cluster ebenso.

Bleiben die Pakete privat, ist der Weg ein Pull-Secret:

```bash
kubectl create secret docker-registry ghcr -n redetim \
  --docker-server=ghcr.io --docker-username=<user> --docker-password=<PAT mit read:packages>
helm upgrade ... --set imagePullSecrets[0].name=ghcr
```

> `imagePullPolicy: Never` wäre hier **schlechter**: es scheitert mit `ErrImageNeverPull`, statt
> überhaupt zu ziehen. Deshalb steht in `values.yaml` `IfNotPresent`.

---

## 7. Installation mit Helm

```bash
REL=$(./scripts/select-release.sh)
VERSION=$(basename "$REL" .yaml)

helm upgrade --install redetim ./deploy/helm/redetim \
  -n redetim --create-namespace --wait --timeout 10m \
  -f "$REL" \
  --description "release $VERSION"

kubectl -n redetim get pods
```

Die Release-Datei ist **Pflicht**, nicht optional. Ohne sie bricht das Chart mit einer klaren
Meldung ab, statt irgendein Image zu starten:

```text
Error: chatClient.image.tag is empty: no release selected.
Run scripts/build-images.sh, then deploy with -f deploy/releases/<version>.yaml
```

Was läuft gerade?

```bash
kubectl -n redetim get pods -L app.kubernetes.io/version
kubectl -n redetim get deploy redetim-backend -o jsonpath='{..image}'
```

**Helm ist der Installationsweg**, und zwar der einzige. Ein gerendertes
`deploy/k8s/rendered.yaml` lag früher daneben und ist entfernt: es konnte die `fail`-Prüfungen
des Charts nicht mitnehmen, mintet bei jedem Rendern eine CA samt zwei privaten Schlüsseln ins
Repository, scheiterte beim zweiten `kubectl apply` am immer gleichen Topic-Job-Namen, und hing
dem Chart zuletzt um jedes fehlende Secret hinterher, ohne dass es jemand bemerkte. Die
vollständige Begründung:
[docs/build.md](docs/build.md#warum-es-kein-gerendertes-manifest-mehr-gibt).

Das Release-Artefakt ist `deploy/releases/<version>.yaml`. Es pinnt den unveränderlichen
Image-Tag, und genau das ist es, was ein späteres `helm rollback` einen echten Build
wiederherstellen lässt. Wer die Manifeste ansehen will, rendert sie sich — installiert wird
daraus nicht:

```bash
helm template redetim deploy/helm/redetim -n redetim -f "$REL" | less
```

Deinstallation:

```bash
helm uninstall redetim -n redetim
```

> Das **PVC des Brokers bleibt absichtlich stehen.** PVCs aus `volumeClaimTemplates` gehören
> dem StatefulSet-Controller, nicht dem Helm-Release; Kubernetes löscht sie bewusst nicht mit.
> Wer wirklich bei null anfangen will: `kubectl -n redetim delete pvc --all`.
>
> Die **TLS-Secrets gehören dagegen dem Release** und verschwinden mit ihm. Eine
> Neuinstallation stellt also eine neue CA aus, und der Browser fragt einmal neu nach.

### Zertifikate

Das Chart stellt sich beim ersten `helm install` eine eigene CA aus und signiert damit zwei
Server-Zertifikate — Frontend und Backend. Jedes landet in einem eigenen Secret vom Typ
`kubernetes.io/tls`, zusammen mit der CA:

```bash
kubectl -n redetim get secret -l app.kubernetes.io/instance=redetim | grep -E 'ca|tls'
# redetim-ca                    Opaque              2
# redetim-backend-tls           kubernetes.io/tls   3
# redetim-frontend-tls          kubernetes.io/tls   3
```

**Warum kein cert-manager.** Er wäre in einem Cluster, der ihn ohnehin betreibt, die richtige
Antwort. Hier wäre er die falsche Abhängigkeit: aus `helm upgrade --install` würde „erst CRDs,
einen Webhook und drei Pods installieren, warten, dann das Chart", und zwar auf kind, minikube
und Docker Desktop — genau den Clustern, für die dieses Chart gedacht ist. Der Preis dieser
Entscheidung ist ehrlich zu nennen: die Rotation ist hier Handarbeit, und der CA vertraut
nichts außerhalb dieses Releases.

**Zertifikate werden wiederverwendet, nicht stillschweigend rotiert.** Ein `helm upgrade` sucht
die vorhandenen Secrets über `lookup` und lässt sie unangetastet; neu ausgestellt wird nur, was
fehlt. Ohne das würde jedes Upgrade sämtliches Schlüsselmaterial austauschen und jeden Pod neu
starten.

**Der CA vertrauen statt wegklicken.** Im Browser genügt es, die Warnung einmal je Port zu
akzeptieren. Auf der Kommandozeile geht es sauberer — `scripts/demo.sh` schreibt die CA beim
Start selbst heraus:

```bash
kubectl -n redetim get secret redetim-ca \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/redetim-ca.crt

curl --cacert /tmp/redetim-ca.crt https://localhost:8443/healthz
```

**Rotieren:** Secrets löschen, upgraden. Die `checksum/tls`-Annotation auf allen vier
Pod-Templates sorgt dafür, dass die Pods dabei wirklich neu starten — sonst würden sie das alte
Zertifikat weiter ausliefern, während die Gegenstellen schon nur noch der neuen CA trauen.

```bash
kubectl -n redetim delete secret redetim-ca redetim-frontend-tls redetim-backend-tls
helm upgrade redetim ./deploy/helm/redetim -n redetim \
  -f deploy/releases/<version>.yaml --wait
```

**Gültigkeit:** CA 3650 Tage, Blätter 825 Tage (`tls.caValidityDays`, `tls.certValidityDays`).
Jedes Blatt trägt den Service-Namen in allen vier Cluster-Schreibweisen als SAN, dazu
`localhost` und `127.0.0.1` — ohne die beiden letzten würde jeder Befehl in diesem README
scheitern, denn sie laufen alle über `kubectl port-forward`.

### Rollback

Jede Installation und jedes Upgrade ist eine Helm-Revision. Weil jede Revision einen eigenen,
unveränderlichen Image-Tag mitbringt, holt ein Rollback wirklich den alten Stand zurück:

```bash
helm history redetim -n redetim
# REVISION  STATUS      DESCRIPTION
# 1         superseded  release 0.1.0-ga1b2c3d
# 2         deployed    release 0.1.0-g103b98b

helm rollback redetim 1 -n redetim
kubectl -n redetim get deploy redetim-frontend -o jsonpath='{..image}'
# redetim-frontend:0.1.0-ga1b2c3d
```

Die Spalte `DESCRIPTION` kommt aus `--description` im Deploy-Befehl. `APP VERSION` taugt dafür
**nicht**: die liest Helm aus `Chart.yaml` und sie ist auf jeder Revision dieselbe.

> Voraussetzung ist, dass das alte Image noch erreichbar ist. Aus `ghcr.io` ist es das, solange
> das Paket nicht gelöscht wurde. Ist es weg, baut man es aus dem Commit neu, der in der
> Release-Datei unter `release.gitSha` steht.

### Schalter in `values.yaml`

| Wert | Default | Wirkung |
|---|---|---|
| `backend.replicas` | `2` | Backend-Pods. Ab 2 überlebt der Chat den Ausfall eines Pods; wird ignoriert, solange `backend.autoscaling.enabled` gesetzt ist |
| `backend.autoscaling.enabled` | `false` | HPA auf CPU-Basis. **Braucht metrics-server**, siehe unten |
| `chat.topic` | `redetim-chat` | Topicname für Backend, Client und Init-Job |
| `chat.maxMessageLength` | `500` | maximale Nachrichtenlänge |
| `chat.historySize` | `200` | Nachrichten pro Raum im Speicher **jedes** Pods; `0` = alles, was im Topic liegt |
| `chat.produceTimeoutMs` | `10000` | Obergrenze für das Senden einer Nachricht; darüber antwortet das Backend mit 504 |
| `frontend.replicas` | `2` | Caddy-Pods. Jeder SSE-Stream läuft hier durch, deshalb wie beim Backend nicht 1 |
| `redpanda.enabled` | `true` | Deployt den Broker mit. `false` ⇒ kein Broker im Chart, siehe unten |
| `redpanda.external.bootstrapServers` | `""` | Pflicht bei `redpanda.enabled=false` |
| `redpanda.auth.securityProtocol` | `Plaintext` | `Ssl`, `SaslPlaintext`, `SaslSsl` — auch in der Schreibweise `SASL_SSL` |
| `redpanda.auth.existingSecret` | `""` | Secret mit `username`/`password`. Pflicht bei einem SASL-Protokoll |
| `redpanda.auth.caSecret` | `""` | Secret mit `ca.crt` für eine private CA (nur bei TLS) |

### Gegen einen fremden Broker deployen

Das ist die Probe auf „austauschbarer Backing Service": ohne diesen Schalter wäre Redpanda kein
Backing Service, sondern ein Bestandteil der Anwendung.

```bash
# Kein Broker im Chart, dafür ein vorhandener anderswo:
helm upgrade --install redetim ./deploy/helm/redetim -n redetim \
  -f deploy/releases/<version>.yaml \
  --set redpanda.enabled=false \
  --set redpanda.external.bootstrapServers=kafka.example.com:9093

# Mit Authentifizierung. Die Zugangsdaten kommen aus einem Secret, das man selbst anlegt --
# nicht über --set, weil Werte in `helm get values` und in der Shell-History landen:
kubectl -n redetim create secret generic broker-creds \
  --from-literal=username=chat --from-literal=password=…

helm upgrade --install redetim ./deploy/helm/redetim -n redetim \
  -f deploy/releases/<version>.yaml \
  --set redpanda.enabled=false \
  --set redpanda.external.bootstrapServers=kafka.example.com:9093 \
  --set redpanda.auth.securityProtocol=SASL_SSL \
  --set redpanda.auth.saslMechanism=SCRAM-SHA-512 \
  --set redpanda.auth.existingSecret=broker-creds
```

Für eine private CA kommt `--set redpanda.auth.caSecret=<Secret mit ca.crt>` dazu; ohne das
benutzt librdkafka den System-Truststore, was für ein öffentlich vertrauenswürdiges Zertifikat
das Richtige ist. Backend, Konsolenclient und Topic-Job lesen dieselben Variablen — der Job
wartet dabei auf *Metadaten* statt auf `rpk cluster health`, weil ein verwalteter Broker keinen
Grund hat, Redpandas Admin-Port zu veröffentlichen.

Zwei Fehler brechen bewusst schon beim Rendern ab, statt als Pod zu erscheinen, der jede
Verbindung scheitern lässt: `redpanda.enabled=false` ohne Adresse, und ein SASL-Protokoll ohne
`existingSecret`.

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
kubectl top pods -n redetim

helm upgrade redetim ./deploy/helm/redetim -n redetim -f "$REL" \
  --set backend.autoscaling.enabled=true
kubectl -n redetim get hpa redetim-backend
```

`TARGETS` muss eine Zahl sein, kein `<unknown>`. Ist der HPA an, lässt das Deployment `replicas:`
weg — sonst schriebe jedes `helm upgrade` den Wert aus `values.yaml` zurück und der Autoscaler
korrigierte ihn sofort wieder.

---

## 8. Demo

```bash
./scripts/demo.sh
```

Öffnet die Port-Forwards für das Frontend (8443 und 8080) und schreibt die Release-CA nach
`/tmp/redetim-ca.crt`.

Alles läuft über TLS mit einer CA, die kein Browser kennt. Beim ersten Aufruf kommt deshalb
eine Zertifikatswarnung — einmal je Port akzeptieren. Wer sie nicht sehen will, importiert die
CA aus `/tmp/redetim-ca.crt` in den eigenen Truststore.

1. **Zwei Browserfenster** auf <https://localhost:8443>, beide Raum `general`, verschiedene
   Namen → beide sehen jede Nachricht.
2. **Zweiter Raum:** ein Fenster auf `andererraum` → keine Vermischung.
3. **Seite neu laden:** der Verlauf des Raums ist wieder da — er kommt aus dem Topic, nicht aus dem
   Browser. Ein drittes Fenster, das den Raum zum ersten Mal betritt, sieht dasselbe.
4. **Netzwerk-Tab öffnen:** das Frontend spricht ausschließlich mit `/api/...`. Kein Kafka. Das
   Schloss-Symbol ist dabei kein Nebenschauplatz, sondern die kürzeste Antwort auf „läuft das
   wirklich über HTTPS": jede Zeile in dieser Liste ist `https`.
5. **Der alte Link tut noch etwas Sinnvolles.** <http://localhost:8080> landet per `308` auf der
   TLS-Adresse, statt in einem Verbindungsfehler:
   ```bash
   curl -sS -o /dev/null -w '%{http_code} -> %{redirect_url}\n' http://localhost:8080/
   # 308 -> https://localhost:8443/
   ```
6. **Konsolenclient** parallel laufen lassen → liest dieselben Nachrichten mit.
7. **Einen von zwei Backend-Pods löschen.** Vier Fenster im selben Raum öffnen (bei zweien ist es
   ein Münzwurf, ob sie überhaupt auf verschiedenen Pods landen — kube-proxy entscheidet das pro
   TCP-Verbindung), dann:
   ```bash
   kubectl -n redetim get pods -l app.kubernetes.io/component=backend
   kubectl -n redetim delete pod <einer der beiden>
   ```
   Die Fenster am **anderen** Pod merken davon nichts. Die am gelöschten verbinden neu, landen auf
   der überlebenden Replica und lesen dort weiter — **ohne** dass der bereits gelesene Verlauf ein
   zweites Mal erscheint. Das ist der Beleg, dass die Skalierung echt ist und nicht behauptet: der
   Offset in `Last-Event-ID` gilt brokerweit, also auch auf einem Pod, der die Verbindung nie
   gesehen hat.
8. **Rollout ohne Ausfall.** Währenddessen weitertippen:
   ```bash
   kubectl -n redetim rollout restart deploy/redetim-backend
   kubectl -n redetim rollout status deploy/redetim-backend
   ```
   Es dürfen nie beide Pods gleichzeitig fehlen (`maxUnavailable: 0`), und im Chat darf weder eine
   Lücke noch eine Dublette entstehen.
9. **Reconnect-Anzeige.** Erst wenn das Backend *ganz* weg ist, zeigt das Frontend sein
   Ausfallverhalten:
   ```bash
   kubectl -n redetim scale deploy/redetim-backend --replicas=0
   ```
   Es zeigt „verbinde neu…", blendet einen Hinweis über dem Eingabefeld ein und sperrt es, bis der
   Stream wieder steht. Es versucht es dabei selbst erneut — mit wachsendem Abstand (1, 2, 4, 8,
   dann am 15-Sekunden-Deckel), insgesamt acht Versuche über rund 75 Sekunden. Bleibt das Backend darüber hinaus weg,
   wechselt die Anzeige auf „getrennt" und der Hinweis bekommt einen Knopf „Erneut verbinden".
   Danach `--replicas=2` zurück.

### Was im Frontend absichtlich so ist

- **Verlauf ohne History-Endpunkt.** `GET /api/stream` schickt den Raumverlauf als erste Frames,
  bevor der Live-Betrieb beginnt. Ein separates `GET /api/history` gibt es bewusst nicht.
- **Kein doppelter Verlauf beim Reconnect.** Jedes Datenframe trägt seinen Kafka-Offset als
  SSE-`id`; der Client filtert zusätzlich selbst danach. Das deckt beide Reconnect-Pfade ab und
  ist die Grundlage für die Punkte 7 bis 9 oben.
- **Kein Build-Tooling, keine externen Requests.** Kein npm, kein CDN, keine Webfonts: vier
  statische Dateien, die Caddy ausliefert. Im Netzwerk-Tab tauchen nur diese und `/api/...` auf —
  Grundlage für Punkt 3 oben, und lauffähig ohne Internetzugang.
- **Hell und dunkel.** Standardmäßig folgt die Oberfläche der Systemeinstellung; der Schalter oben
  rechts erzwingt ein Schema und merkt es sich. Praktisch für Screenshots.
- **Farbe pro Name.** Der Farbton wird aus dem Nickname gehasht. Eigene Nachrichten erkennt man an
  Position und Label „(du)", nicht an der Farbe allein.

Warum es zwei Reconnect-Pfade gibt und warum der Client trotz `Last-Event-ID` zusätzlich filtern
muss: [docs/frontend.md](docs/frontend.md#zwei-reconnect-pfade-nicht-einer).

---

## 9. Konfiguration

Die laufzeitvariablen Anwendungsparameter kommen aus Umgebungsvariablen. Im Cluster stammen die
nicht geheimen Werte überwiegend aus einer ConfigMap. Zugangsdaten werden per `secretKeyRef` aus
einem Secret injiziert, `POD_NAME` kommt über die Downward API, und TLS-Zertifikate werden als
Secret-Dateien gemountet.

| Variable | Default | Verwendet von |
|---|---|---|
| `REDPANDA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Backend, Konsolenclient |
| `REDPANDA_TOPIC` | `redetim-chat` | Backend, Konsolenclient, Topic-Job |
| `REDPANDA_SECURITY_PROTOCOL` | `Plaintext` | Backend, Konsolenclient, Topic-Job |
| `REDPANDA_SASL_MECHANISM` | — (Pflicht bei SASL) | dieselben drei |
| `REDPANDA_SASL_USERNAME` | aus dem Secret (Pflicht bei SASL) | dieselben drei |
| `REDPANDA_SASL_PASSWORD` | aus dem Secret (Pflicht bei SASL) | dieselben drei |
| `REDPANDA_SSL_CA_LOCATION` | — (nur bei privater CA) | dieselben drei |
| `MAX_MESSAGE_LENGTH` | `500` | Backend |
| `CHAT_HISTORY_SIZE` | `200` (`0` = alles im Topic) | Backend (Nachrichten **je Raum**) |
| `CHAT_MAX_ROOMS` | `200` (`0` = unbegrenzt) | Backend (Räume gleichzeitig im Verlauf) |
| `CHAT_REPLAY_RECORDS` | `2000` (`0` = alles im Topic) | Backend (Rücklesen beim Start, **je Partition**) |
| `REDPANDA_PRESENCE_TOPIC` | `redetim-presence` | Backend, Topic-Job (log-komprimiert, Präsenz) |
| `PRESENCE_TTL_SECONDS` | `45` | Backend (Reservierung ohne Erneuerung wird nach dieser Zeit frei) |
| `PRESENCE_PARTITIONS` | `1` | Topic-Job |
| `PRESENCE_REPLICATION_FACTOR` | `1` | Topic-Job |
| `PRODUCE_TIMEOUT_MS` | `10000` | Backend (Producer, `message.timeout.ms`) |
| `CHAT_PARTITIONS` | `1` | Topic-Job |
| `CHAT_REPLICATION_FACTOR` | `1` | Topic-Job |
| `TOPIC_WAIT_SECONDS` | `180` | Topic-Job (Warten auf den Broker) |
| `ASPNETCORE_URLS` | `https://+:8443` (Image: `http://+:8080`) | Backend |
| `ASPNETCORE_Kestrel__Certificates__Default__Path` | `/etc/redetim/tls/tls.crt` | Backend (Kestrel) |
| `ASPNETCORE_Kestrel__Certificates__Default__KeyPath` | `/etc/redetim/tls/tls.key` | Backend (Kestrel) |
| `POD_NAME` | im Cluster Pflicht (fieldRef), lokal `MachineName-PID` | Backend (Consumer-GroupId) |
| `LOG_LEVEL` | `Information` | Backend |
| `BACKEND_HOST` | `redetim-backend:8443` | Frontend (Caddyfile) |
| `PUBLIC_HTTPS_PORT` | `8443` | Frontend (Ziel-Port der `308`-Weiterleitung) |
| `FRONTEND_LOG_LEVEL` | `INFO` | Frontend (Caddy: Access-Log **und** Runtime-Log) |

`FRONTEND_LOG_LEVEL` kennt `DEBUG`, `INFO`, `WARN` und `ERROR` — bewusst eine andere Schreibweise
als `LOG_LEVEL` (`Information`). Das ist Caddys Vokabular, nicht das von .NET, und Caddy weist die
.NET-Schreibweise beim Start zurück, statt still auf einen Default zurückzufallen. Die beiden
Variablen zu vereinheitlichen hieße, eine der beiden Laufzeiten anzulügen.

Die Anwendungsvariablen bis einschließlich `LOG_LEVEL` liest das Backend **explizit** in
`BackendOptions` aus, statt sich auf das `Section__Key`-Autobinding von ASP.NET zu verlassen — nur
so stimmen die schlichten Namen aus dieser Tabelle mit dem Code überein.

Die beiden `ASPNETCORE_Kestrel__…`-Variablen sind die einzige Ausnahme von dieser Schreibweise:
Kestrels Zertifikat ist Konfiguration des Frameworks, das Framework liest sie selbst, und sie in
`BackendOptions` noch einmal zu lesen hieße, eine
zweite Wahrheit für etwas anzulegen, das der Anwendung gar nicht gehört. Deshalb steht auch bei
`ASPNETCORE_URLS` ein zweiter Wert in der Tabelle: das Image bringt weiterhin
`http://+:8080` mit, weil ein Image, das ein Zertifikat an einem festen Pfad verlangt, ohne
Cluster nicht startbar wäre. Das Chart überschreibt beides.

Die fünf Verbindungsvariablen (`REDPANDA_SECURITY_PROTOCOL` bis `REDPANDA_SSL_CA_LOCATION`) liest
dagegen `KafkaSecurity` in `RedeTim.Contracts` — einmal für *jeden* Kafka-Client im Repo, weil
`ClientConfig` die gemeinsame Basis von Producer-, Consumer- und Admin-Konfiguration ist. Ohne
Konfiguration ändert die Klasse nichts; unvollständig konfiguriert bricht sie beim Start ab und
nennt die fehlende Variable.

Hier stand einmal eine feste Zahl, und sie war jedes Mal wieder falsch — zuletzt, als mit
`--describe-topic` zwei Clients verschwanden. Deshalb steht jetzt keine mehr, sondern die Prüfung:
`BrokerReadinessTests` geht jeden Client des Backends einzeln durch, und ein Reflection-Test darin
findet einen neuen von selbst. Die Herleitung steht in
[docs/kafka.md](docs/kafka.md#abgesicherte-broker). Zugangsdaten stehen dabei **nie** in der
ConfigMap: sie kommen über `secretKeyRef` aus einem Secret, das man selbst anlegt (Schlüssel
`username`, `password`).

`POD_NAME` ist dabei die einzige Variable ohne brauchbaren Default: die Consumer-GroupId wird
daraus gebildet, und zwei Replicas in *einer* Gruppe teilen sich nicht die Last, sie legen
einander still — Kafka gibt die eine Partition an genau einen Pod, alle Browser an den übrigen
sähen einen Raum, der nie mehr etwas anzeigt. Im Cluster kommt der Name aus einem `fieldRef` auf
`metadata.name` (pro Namespace garantiert eindeutig); fehlt er dort, startet der Pod **nicht**,
statt die Gruppe stillschweigend zu kollidieren. Außerhalb von Kubernetes gibt es keinen
`fieldRef`, dafür aber denselben Fehlerfall — zweimal `dotnet run` auf einer Maschine —, weshalb
der lokale Default die Prozess-ID enthält.

---

## 10. Umgesetzte 12-Factor-Prinzipien

| Faktor | Umsetzung | Einschränkung |
|---|---|---|
| Codebase | ein Git-Repo, **eine** Beschreibung des Deployments (das Chart), sieben Wertekombinationen rendern sauber daraus | ein gerendertes Manifest ist nicht reproduzierbar: `tls.yaml` findet ohne Cluster nichts nachzuschlagen und mintet bei jedem Lauf neue Schlüssel. Deshalb liegt keines im Repo (Abschnitt 7) |
| Dependencies | NuGet zentral deklariert und per `packages.lock.json` inklusive transitiver Pakete festgenagelt, Restore nur gegen nuget.org; alle Registry-Images per Digest gepinnt; Frontend bewusst ohne Build-Tooling (Vanilla JS) | CI ruft `./scripts/check-repro.sh` und `dotnet test -p:ContinuousIntegrationBuild=true` bei jedem Push und PR auf. Im Alltag schreibt ein `dotnet test` die Lock-Dateien weiterhin um, statt zu scheitern — absichtlich, siehe Abschnitt 12 |
| Config | laufzeitvariable Anwendungsparameter über Env-Variablen, im Cluster überwiegend aus der ConfigMap; Zugangsdaten getrennt davon per `secretKeyRef` aus einem Secret (`redpanda.auth.existingSecret`), `POD_NAME` aus der Downward API und TLS-Zertifikate als Secret-Mounts, nie geheime Werte aus `values.yaml` | kein Live-Reload: eine geänderte ConfigMap rollt die Pods über die `checksum/config`-Annotation, sie wird nicht im laufenden Prozess nachgelesen |
| Backing Services | Redpanda über `REDPANDA_BOOTSTRAP_SERVERS` — ohne Codeänderung austauschbar, **und auch im Chart**: `redpanda.enabled=false` + `redpanda.external.bootstrapServers`. TLS/SASL sind über `redpanda.auth` konfigurierbar und in Abschnitt 5 gegen einen echten SASL/TLS-Broker vorgeführt | der mitgelieferte Broker spricht weiterhin Plaintext, und das Chart lehnt die Kombination „abgesichertes Protokoll + mitgelieferter Broker" beim Rendern ab, weil sie nicht funktionieren kann. Es ist der einzige angehängte Dienst — mit der Telemetriestrecke fiel der zweite Beleg für diesen Faktor weg |
| Build, Release, Run | drei getrennte Stufen mit identifizierbarem Release: unveränderlicher Image-Tag + Release-Datei, `helm rollback` funktioniert (siehe unten) | die Release-Datei wird von CI **einen Commit nach** dem gebauten Commit committet — `main` trägt nie eine Release-Datei für die eigene Spitze. Die Images sind amd64-only, weil der Runner es ist. CI hat keinen Cluster: die Abnahmeliste in Abschnitt 12 bleibt manuell |
| Processes | kein dauerhafter lokaler Zustand; SSE-Verbindungen sind bewusst prozesslokal | der Verlaufspuffer ist nur eine Projektion des Topics, die jeder Pod beim Start neu aufbaut. Ein Leser, der weiter als 256 Nachrichten zurückfällt, bekommt seinen Stream **beendet** statt still gekürzt — der Browser verbindet sich neu und holt die Lücke per `Last-Event-ID` nach |
| Port Binding | Backend `:8443`, Frontend `:8443` (plus `:8080` nur für die `308`-Weiterleitung), kein externer Webserver nötig. TLS terminiert der Prozess selbst — es gibt keinen vorgelagerten Terminator, den das Deployment mitbringen müsste | das Zertifikat kommt als Secret-Mount von außen; das Image allein kann kein TLS und startet deshalb per Default auf `:8080` |
| Concurrency | **Beide** Deployments laufen mit 2 Replicas, PodDisruptionBudget, `preStop`-Drain und Rollout ohne Unterbrechung (`maxUnavailable: 0`) — der SSE-Pfad ist damit von Caddy bis Kafka redundant, nicht nur an seinem hinteren Ende. Backend zusätzlich: Consumer-GroupId pro Pod ⇒ echter Fan-out, HPA optional, kein Sticky-Session-Bedarf, weil die SSE-`id` der Kafka-Offset ist. Caddy spricht zum Backend auf `versions 1.1` — über HTTP/2 liefe jeder Stream eines Pods über *eine* TCP-Verbindung und damit auf *einer* Backend-Replica | Offsets sind **pro Partition** eindeutig, nicht brokerweit. Bei `chat.partitions > 1` trägt die Konstruktion nur, weil beide Producer nach Raum keyen und ein Raum damit auf einer Partition bleibt. Jede Replica liest 100 % des Topics: der Fan-out skaliert das Ausliefern, nicht das Lesen. HPA braucht metrics-server und ist per Default aus |
| Disposability | SIGTERM: Consumer `Close()` (auf 5 s begrenzt, sonst wird der Broker nicht mehr abgewartet), Producer `Flush()`, offene SSE-Streams enden über `ApplicationStopping` statt bis zum Timeout weiterzuheartbeaten. Frontend analog: Caddy hat `grace_period 5s`, sonst wartete es unbegrenzt auf SSE-Antworten, die per Definition nie fertig werden | das Budget ist ≈ 35 s, nicht 30: `ChatProducer.Dispose()` flusht bis zu 5 s **nachdem** `Host.StopAsync` zurückgekehrt ist, also `preStop` 5 s + 25 s + 5 s < Grace Period 45 s. Der `Close()`-Term von 5 s liegt *innerhalb* der 25 s und zählt nicht doppelt (Aufstellung: [docs/kafka.md](docs/kafka.md#herunterfahren)). Der `Close()`-Term ist neu begrenzt: `HostOptions.ShutdownTimeout` bricht ihn nicht ab (der Timeout kündigt ein Token, das dieser Aufruf gar nicht entgegennimmt), also lief er gegen einen unerreichbaren Broker über das ganze Budget hinaus in ein stilles SIGKILL. Die Readiness hängt am Broker: ohne erreichbaren Broker wird der Pod nie `Ready` — richtig so, aber es macht eine Broker-Störung zu einem Rollout, der stehen bleibt |
| Dev/Prod Parity | derselbe Broker lokal und im Cluster, **inklusive Digest** und im selben `--mode=dev-container`; identisch gepinntes .NET-SDK in `global.json`, `flake.nix` und beiden Build-Dockerfiles | **die Anwendungs-Images laufen lokal nicht**: Abschnitt 5 startet Backend und Konsolenclient per `dotnet run` aus dem Quelltext, und das Frontend läuft lokal überhaupt nicht — die Browser-Oberfläche gibt es nur über den Cluster-Weg. Redpanda ist ein einzelner Broker ohne Replikation |
| Logs | strukturiert (JSON) nach stdout, keine Logdateien: Backend über `AddJsonConsole` mit `Timestamp`/`LogLevel`/`Category`, Frontend als Caddy-Access-Log (eine Zeile pro Request, Probes ausgenommen). Auch librdkafkas eigene Ausgabe geht über `SetLogHandler` durch `ILogger`, statt roh auf stderr an `LOG_LEVEL` vorbei | Logs laufen bewusst **nicht** über OTLP. Die beiden JSON-Schemata sind nicht vereinheitlicht — Caddy loggt zap-artig (`ts`/`level`/`msg`/`request`), .NET mit `Timestamp`/`LogLevel`/`Category`. Und der Chat-Client ist gar nicht strukturiert — Admin-Prozess wie interaktiver Betrieb: er schreibt einfache Zeilen mit `Console.WriteLine` und kennt kein `LOG_LEVEL` |
| Admin Processes | `--ensure-topic` läuft aus **demselben Build unter demselben Tag** wie die Anwendung und mit **derselben ConfigMap** per `envFrom` — bei jedem Install und Upgrade als Job. Es legt den Topic an und zieht die Partitionszahl nach. Kein Shell-Skript in einem fremden Image, keine zweite Konfigurationsquelle | „dasselbe Image" wäre zu viel gesagt: es sind zwei Images auf zwei Laufzeit-Basen (`aspnet:10.0` für das Backend, `runtime:10.0` für den Client). Gemeinsam sind Build, Tag und Konfiguration — und darauf kommt es an. Der Job-Name trägt die Release-Revision, das Pod-Template ist unveränderlich. Ein zweiter Job mit frei setzbaren Argumenten stand hier einmal; ihn hat nie jemand aufgerufen |

### Build, Release, Run im Einzelnen

Die drei Stufen sind strikt getrennt, und zwischen ihnen wird nichts von Hand angefasst.

| Stufe | Wer | Ergebnis |
|---|---|---|
| **Build** | `./scripts/build-images.sh --release` | drei Images unter `…:0.1.0-g103b98b` (Backend, Frontend, Konsolenclient/Admin-Prozess) und `deploy/releases/0.1.0-g103b98b.yaml` |
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
- Es gibt genau **eine** Beschreibung des Deployments, nämlich das Chart. Ein zweites,
  gerendertes Manifest lag hier früher daneben und war irgendwann eine Version hinterher — zwei
  Beschreibungen desselben Systems laufen zuverlässig auseinander (Abschnitt 7).

Eine Registry und ein CI, das den Build anstößt, fehlten hier lange. Beides gibt es jetzt:
`scripts/build-images.sh --push` schiebt die drei Images (Backend,
Frontend und Konsolenclient — es waren einmal zwei, bevor der Admin-Prozess ein eigenes Image
bekam) nach `ghcr.io/dianatin23/`, und `.github/workflows/release.yml` stößt genau das an. Was
noch fehlt: ein Digest-Pin auch für diese drei Images. Der Tag ist unveränderlich, weil ihn
niemand zweimal vergibt — nicht, weil die Registry es erzwänge.

---

## 11. Eingesetzte CNCF-Technologien

- **Kubernetes** (*graduated*) — Laufzeitplattform.
- **Helm** (*graduated*) — die gesamte Anwendung wird darüber installiert, aktualisiert und
  deinstalliert; Parametrisierung über `values.yaml`.
- **Redpanda** — der Broker, über den die beiden Anwendungen ausschließlich miteinander reden.
  In der CNCF-Landscape gelistet, aber **kein** CNCF-gehostetes Projekt und nicht Open Source im
  engeren Sinne (BSL 1.1, Apache-2.0 nach vier Jahren).

**OpenTelemetry** und **Prometheus** waren einmal Teil des Projekts: das SDK im Backend erzeugte
fünf fachliche Metriken, ein OTel-Collector übersetzte sie, Prometheus scrapte ihn. Beides ist
bewusst wieder entfernt worden — das Projekt soll auf **einem** Landscape-Produkt stehen, und
Redpanda ist dieses Produkt. Die Anwendung erzeugt seitdem überhaupt keine Telemetrie mehr;
Logs gehen nach stdout, wie es 12-Factor vorsieht.

**Grafana** wäre ebenfalls Landscape, aber nicht CNCF-gehostet — hier nicht eingesetzt.

---

## 12. Tests und Abnahme-Checkliste

```bash
dotnet test -p:ContinuousIntegrationBuild=true  # 197 Tests, locked mode wie in CI
./scripts/validate-chart.sh                     # beide HPA-Varianten, replicas, Negativfall
./scripts/check-repro.sh                        # alle vier Projekte gegen ihre Lock-Dateien
./scripts/check-digests.sh                      # Image-Digests + Broker-Parität lokal/Cluster
```

Was `validate-chart.sh` prüft und warum jede Prüfung dort steht, sagt sein `--help`. Kurz: das
Chart wird **zweimal** gerendert, weil der HPA per Vorgabe aus ist und `backend-hpa.yaml` sonst
nie jemand validiert; `replicas` muss ohne HPA dem Wert aus `values.yaml` entsprechen und mit
HPA fehlen; und Rendern **ohne** Release-Datei muss abbrechen. `helm lint` allein genügt dafür
nicht — Helm 4 stuft ein `fail` im Template auf INFO herab und meldet trotzdem „0 chart(s)
failed".

`check-repro.sh` ist die Stelle, an der die Antwort auf eine abweichende Lock-Datei „Fehler"
lautet statt „umschreiben". Warum der Locked-Mode im Alltag aus ist und was ohne diese Stelle
passierte, steht in [docs/build.md](docs/build.md#lockfiles-und-wann-sie-tatsächlich-etwas-erzwingen).

Das Release selbst schneidet `--release`, und es verweigert den Dienst, solange der Arbeitsbaum
nicht sauber ist:

```bash
./scripts/build-images.sh --release
```

Ein gerendertes Manifest entsteht dabei bewusst nicht mehr (Abschnitt 7). Wer die Manifeste
prüfen will, rendert sie in die Pipe — sie sind ein Prüfobjekt, kein Installationsweg. Beim
Rendern entstehen die Zertifikate jedes Mal neu, weil `lookup` ohne Cluster nichts findet; die
drei Secrets sind deshalb bei jedem Lauf andere und taugen nicht zum Vergleichen.

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

### Was CI davon selbst anstößt

Jede Sache hat ihren eigenen Workflow unter `.github/workflows/`; zusammen fahren sie bei jedem
Push und PR den ganzen Block oben, außer dem, was einen Cluster braucht:

| Workflow | Trigger | Was er prüft |
|---|---|---|
| `dotnet.yml` | Push auf `main` (ohne `deploy/releases/**`), PR, `workflow_dispatch`, `workflow_call` | `check-repro.sh`, `dotnet test -p:ContinuousIntegrationBuild=true`, Lock-Dateien unverändert |
| `chart.yml` | wie `dotnet.yml` | ruft `./scripts/validate-chart.sh --committed-only` auf: beide HPA-Varianten, die `replicas`-Kopplung, der Fall ohne Release-Datei, der abbrechen muss |
| `release.yml` | nur `workflow_dispatch` auf `main` | baut, pusht nach `ghcr.io`, committet die Release-Datei zurück |
| `digests.yml` | wöchentlich, `workflow_dispatch` | `check-digests.sh` |

Ein rotes Ergebnis in `digests.yml` heißt „Digest gewandert **oder** Manifest nicht lesbar" —
also erst den Log lesen, dann glauben.

`release.yml` läuft bei einem Push nicht mit; ausgelöst wird er von Hand. Bevor er baut, ruft er
`dotnet.yml` und `chart.yml` per `workflow_call` auf und hängt per `needs` daran — die
Verifikation ist Vorbedingung, auch wenn sie inzwischen in eigenen Dateien steht. Die
Release-Datei committet er zurück als **Kind** des Commits, den sie beschreibt: die Images zu
einem Commit existieren erst nach ihm.

### Manuell durchzugehen

- [ ] `helm install` auf frischem Cluster → alle Pods `Ready` ohne Eingriff
- [ ] Topic-Job `Completed`
- [ ] Zwei Browserfenster, gleicher Raum → beide sehen die Nachricht
- [ ] Zwei Browserfenster, verschiedene Räume → keine Vermischung
- [ ] Konsolenclient liest dieselben Nachrichten mit
- [ ] Browser neu laden → der Verlauf des Raums ist wieder da, in richtiger Reihenfolge
- [ ] `kubectl delete pod <backend>` → nach dem Reconnect **keine** doppelten Nachrichten
- [ ] Leere Nachricht und Nachricht > `MAX_MESSAGE_LENGTH` → HTTP 400
- [ ] Als Nickname `claude` (auch `Claude`, `CLAUDE`) betreten → HTTP 400, Raum wird nicht
      betreten
- [ ] `GET /api/stream?room=X&nickname=claude` direkt aufrufen (ohne vorheriges `POST /api/join`)
      → Stream öffnet trotzdem (Presence ist keine Sicherheitsgrenze), aber `claude` taucht
      **nicht** in `GET /api/presence?room=X` auf
- [ ] Zwei Browserfenster, gleicher Raum, gleicher Nickname → das zweite `POST /api/join` liefert
      HTTP 409, unabhängig davon, welcher Backend-Pod die Anfrage bedient
- [ ] Tab mit aktivem Nickname schließen (bzw. „Raum verlassen") → Nickname ist sofort wieder frei
      nutzbar, ohne auf `PRESENCE_TTL_SECONDS` zu warten
- [ ] Zwei Browserfenster, gleicher Raum, verschiedene Nicknames → beide sehen binnen
      `PRESENCE_POLL_MS` (10 s, Konstante in `app.js` — keine Umgebungsvariable) den jeweils
      anderen in der „wer ist da"-Anzeige der Kopfzeile
- [ ] Nachricht mit Shift+Enter über mehrere Zeilen verfassen → Enter allein sendet, Shift+Enter
      fügt einen Zeilenumbruch ein, der Umbruch bleibt in der gesendeten Nachricht erhalten
- [ ] Emoji in die Nachricht eingeben (System-Emoji-Picker oder eingefügt) und senden →
      erscheint unverändert bei allen Verbindungen
- [ ] `rpk topic delete redetim-presence` bei laufendem Backend → Chat funktioniert weiter,
      `/health/ready` bleibt `ready`, neue `POST /api/join`-Aufrufe schlagen fehl oder lassen
      jeden Namen zu, bis der Topic-Job das Topic beim nächsten `helm upgrade` neu anlegt
- [ ] Frontend hat keinerlei Kafka-Zugriff (Netzwerk-Tab: nur `/api`)
- [ ] `kubectl get pods -l app.kubernetes.io/component=backend` → **zwei** Pods, beide `Ready`
- [ ] `kubectl logs -l app.kubernetes.io/component=backend --prefix` beim Senden einer Nachricht
      → **beide** Pods loggen denselben Empfang, jeder unter seiner eigenen Consumer-Group
- [ ] **Einen von zwei** Backend-Pods löschen → die Fenster am anderen Pod merken nichts, die am
      gelöschten setzen ohne doppelten Verlauf auf der überlebenden Replica auf
- [ ] `kubectl rollout restart deploy/redetim-backend` während des Tippens → keine Lücke, keine
      Dublette, nie beide Pods gleichzeitig weg
- [ ] `kubectl get pdb redetim-backend` → `ALLOWED DISRUPTIONS` = 1
- [ ] `kubectl get pods -l app.kubernetes.io/component=frontend` → **zwei** Pods; einen davon
      löschen, während zwei Fenster streamen → keins der beiden verliert seinen Stream
- [ ] `kubectl get pdb redetim-frontend` → `ALLOWED DISRUPTIONS` = 1
- [ ] `kubectl scale sts/redpanda --replicas=0`, dann eine Nachricht senden → binnen ~10 s eine
      Fehlermeldung (HTTP 504), **nicht** ein Fenster, das minutenlang hängt. Danach wieder auf 1
- [ ] `kubectl get job -o jsonpath='{..containers[0].image}'` → `redetim-chatclient`, nicht das
      Redpanda-Image; `helm upgrade` ein zweites Mal → der Job loggt „already exists" und wird
      `Completed`
- [ ] `kubectl exec deploy/redetim-backend -- env | grep POD_NAME` → gesetzt. Zum Gegenprobieren
      das `env:`-Feld aus dem Deployment entfernen → der Pod startet **nicht** und sagt, warum
- [ ] Gegen einen fremden Broker: `--set redpanda.enabled=false --set
      redpanda.external.bootstrapServers=…` → kein Broker-StatefulSet, Backend wird trotzdem
      `Ready`, Topic-Job `Completed`
- [ ] `scale --replicas=0` → Banner, Backoff und „Erneut verbinden" sind weiterhin vorführbar
- [ ] Nur mit metrics-server: `kubectl top pods -n redetim` liefert Zahlen und die HPA-`TARGETS`
      sind keine `<unknown>`
- [ ] `kubectl -n redetim logs deploy/redetim-frontend` zeigt je eine JSON-Zeile pro
      Browser-Request — und **keine** für `/healthz`, obwohl beide Probes alle 10 s pollen
- [ ] `curl --cacert /tmp/redetim-ca.crt https://localhost:8443/healthz` → `ok`, **ohne** `-k`
- [ ] `curl -o /dev/null -w '%{http_code} %{redirect_url}' http://localhost:8080/` → `308` auf die
      `https`-Adresse
- [ ] Zertifikate überleben ein Upgrade: den Hash vor und nach `helm upgrade` vergleichen
      (`kubectl get secret redetim-backend-tls -o jsonpath='{.data.tls\.crt}' | sha256sum`) →
      identisch, und `kubectl get pods` zeigt keine neuen Neustarts
- [ ] Rotation greift: die drei TLS-Secrets löschen, `helm upgrade` → beide Deployments
      rollen, danach verifiziert `curl --cacert` wieder mit der **neuen** CA
- [ ] Im Netzwerk-Tab des Browsers ist jede Zeile `https`, inklusive des SSE-Streams
- [ ] `helm upgrade` ohne `-f deploy/releases/<version>.yaml` bricht mit der Tag-Meldung ab
- [ ] **Rollback-Probe:** zweites Release mit sichtbarer Änderung bauen und deployen →
      `helm history` zeigt beide, `helm rollback redetim 1` → `kubectl get deploy
      redetim-frontend -o jsonpath='{..image}'` nennt wieder den **alten** Tag und der
      Browser zeigt den alten Stand
- [ ] `helm uninstall` entfernt alles bis auf das PVC
- [ ] README von einer unbeteiligten Person nachvollzogen
- [ ] **Registry-Probe:** die drei GHCR-Pakete auf *public* gestellt und mit dem Repo verknüpft;
      dann `docker logout ghcr.io && docker pull ghcr.io/dianatin23/redetim-backend:<tag>` zieht
      ohne Anmeldung
- [ ] **Pull-Pfad:** frischer kind-Cluster, **nichts** geladen, nur `helm upgrade --install -f
      deploy/releases/<version>.yaml` → alle Pods `Ready`. Prüft Sichtbarkeit,
      `imagePullPolicy` und den Pull in einem Zug
- [ ] Repository public, Gruppenmitglieder eingetragen

---

## 13. Bekannte Einschränkungen

- **Der HPA ist per Default aus.** Er misst gegen `metrics.k8s.io`, das weder kind noch Docker
  Desktop mitbringen. Angeschaltet skaliert er auf CPU, nicht auf der Zahl der offenen
  SSE-Verbindungen: die gibt es seit dem Rückbau der Telemetrie nirgends mehr als Metrik, und sie
  zu lesen bräuchte erst wieder eine Telemetriestrecke und dann prometheus-adapter oder KEDA. Ein
  Ausbaupfad, keine Lücke.
- **Der Fan-out ist dokumentiert, aber nicht mehr metrisch vorgeführt.** Solange es Prometheus
  gab, belegte `sum(rate(redetim_messages_received_total[1m])) /
  sum(rate(redetim_messages_sent_total[1m])) ≈ Replica-Zahl` die Invariante „eine Consumer-Group
  je Pod" als *gemessene* Zahl. Mit dem Rückbau der Telemetrie ist dieser Beleg ersatzlos
  entfallen. Die Invariante gilt unverändert und ist in Code und Tests festgehalten
  ([docs/kafka.md](docs/kafka.md#eine-consumer-group-je-pod)); vorführen lässt sie sich jetzt nur
  noch indirekt über die Demo-Punkte 7 bis 9 aus Abschnitt 8 und über die Pod-Logs.
- **`topologySpreadConstraints` sind auf einem Ein-Node-Cluster beweisbar wirkungslos.** Sie stehen
  als `ScheduleAnyway` im Chart, weil eine harte Constraint die zweite Replica dort für immer
  `Pending` ließe. Auf einem Node ist das dokumentierter Code, kein getestetes Verhalten — und ein
  `kubectl drain` nimmt dort ohnehin beide Pods mit, PodDisruptionBudget hin oder her.
- **Der Verlauf liegt im Speicher jedes Pods**, und damit `replicas`-mal im Cluster. Jeder Pod hält
  ihn pro Raum vor; `CHAT_HISTORY_SIZE` begrenzt das auf 200 Nachrichten **pro Raum**. Die Startzeit
  begrenzt eine zweite, getrennte Zahl: `CHAT_REPLAY_RECORDS` (Default 2000) sagt, wie weit ein
  startender Pod **pro Partition** zurückliest. Zwei Zahlen, weil sie Verschiedenes zählen — wer pro
  Partition genau `CHAT_HISTORY_SIZE` Sätze zurückliest, füllt keinen Raum mehr ganz, sobald zwei
  Räume aktiv sind.
  Drei ehrliche Grenzen: Der Verlauf hält höchstens `CHAT_MAX_ROOMS` Räume (Default 200) und
  verwirft beim nächsten neuen Raum den am längsten inaktiven; mit `0` ist diese Zahl unbegrenzt.
  Wer länger als 200 Nachrichten weg war, bekommt beim Wiedereinstieg eine **Lücke** statt einer
  Dublette. Und ein Raum, der weiter als `CHAT_REPLAY_RECORDS` zurückliegt, ist nach einem
  Pod-Neustart aus dessen Sicht leer, obwohl er im Topic noch steht. Mit `0` liest der Pod wieder
  den ganzen Topic, also das alte Verhalten.
- **Der Pod wird erst `Ready`, wenn der Verlauf geladen ist.** Richtig so — sonst bekäme der erste
  Besucher nach einem Rollout eine halbe Historie —, aber es koppelt die Readiness an den Broker:
  existiert der Topic noch nicht, bleibt der Pod so lange `NotReady`, bis der Topic-Job durch ist.
- **Redpanda im `dev-container`-Modus**, ein einzelner Broker ohne Replikation. Für eine Demo
  richtig, für Produktion nicht.
- **Der Broker-Pod läuft ohne `readOnlyRootFilesystem`**, als einziger im Chart:
  `rpk redpanda start` schreibt bei jedem Start die zusammengeführte Konfiguration nach
  `/etc/redpanda/redpanda.yaml`.
- **Kein Ingress und keine Authentifizierung.** Port-Forward genügt für die Demo. TLS gibt es
  dagegen auf jeder HTTP-Strecke — mit der einen folgenden Ausnahme.
- **Die Nickname-Sperre pro Raum ist Best-Effort, nicht linearisierbar.** `POST /api/join` prüft
  und reserviert nicht atomar, und die periodische Erneuerung während des Streams prüft beim
  Auffrischen nicht erneut, ob die Reservierung noch demselben Aufrufer gehört — ein seltenes
  Rennen kann einen Namen zurückerobern. Ohne Authentifizierung (siehe oben) umgeht außerdem ein
  roher `curl POST /api/messages` die Sperre vollständig, weil sie nur der Join-Endpunkt und die
  SSE-Verbindung durchsetzen. Das Presence-Topic degradiert zudem bewusst weich: scheitert sein
  Consumer dauerhaft, bleibt der Pod `Ready` und Chat funktioniert weiter, aber die Sperre wird
  bis zur Erholung wirkungslos (Details: [docs/kafka.md](docs/kafka.md#presence-topic)). Für
  eine Demo ohne Anmeldung ist das die richtige Abwägung, nicht die einer Bank.
- **Der mitgelieferte Broker spricht Plaintext.** `redpanda.auth.securityProtocol` steht auf
  `Plaintext`, weil der Broker ein Cluster-interner Service ohne Zertifikat ist. Die Wege für `Ssl`
  und `SaslSsl` sind vorhanden, konfigurierbar (Abschnitt 7) und gegen einen echten SASL/TLS-Broker
  vorgeführt (Abschnitt 5) — im Cluster selbst laufen sie aber weiterhin nicht, weil dort kein
  abgesicherter Broker steht. Das Chart lehnt die Kombination inzwischen beim Rendern ab, statt sie
  zu deployen.
- **Die CA ist so viel wert wie das Cluster.** Der private Schlüssel liegt als Secret daneben,
  damit ein späteres Upgrade ein Zertifikat nachsignieren kann; wer das Secret lesen darf, kann
  jedes Zertifikat dieses Releases ausstellen. Für eine Demo richtig, für Produktion gehört diese
  Aufgabe zu cert-manager oder einer echten PKI.
- **Ein gerendertes Manifest gehört nicht ins Repository**, solange TLS nicht abschaltbar ist:
  `helm template` mintet mangels `lookup` jedes Mal eine frische CA und zwei private Schlüssel,
  und die stünden dann hier. Genau deshalb gibt es `deploy/k8s/rendered.yaml` nicht mehr
  (Abschnitt 7). Rendern zum Ansehen ist in Ordnung; installiert wird über Helm.
- **CI hat keinen Cluster.** `dotnet.yml` prüft Tests und Lock-Dateien, `chart.yml` das Chart;
  die Abnahmeliste aus Abschnitt 12 bleibt vollständig manuell. Genau aus dieser Lücke kamen
  die beiden Fehler, die dieses Repo zuletzt hatte — ein gerendertes Manifest, das dem Chart
  um mehrere hundert Zeilen und fünf Secrets hinterherhing, und ein Kafka-Client, der als
  einziger ohne Sicherheitseinstellungen gebaut wurde. Nur der zweite wäre heute
  aufgefallen (`BrokerReadinessTests`).
- **Die Release-Datei ist ein Kind-Commit.** Der `release`-Job committet sie *nach* dem Commit,
  den sie beschreibt: `main` trägt nie eine Release-Datei für die eigene Spitze, sondern für
  deren Eltern. Anders geht es nicht — der Dateiname enthält den Commit, den es zum Zeitpunkt
  des Schreibens noch nicht gäbe.
- **Die Images sind amd64-only.** Der GitHub-Runner ist amd64, und gebaut wird ohne buildx. Auf
  Apple Silicon bedeutet das Emulation, im schlechtesten Fall einen kind-Cluster, der nicht
  startet. Der Ausbaupfad wäre eine Zwei-Runner-Matrix (`ubuntu-latest` + `ubuntu-24.04-arm`)
  mit `docker buildx imagetools create`.
- **`check-digests.sh` kann rot werden, ohne dass etwas gedriftet ist.** „Digest gewandert" und
  „Manifest nicht lesbar" teilen sich Exit-Code 1, und ein anonymes Docker-Hub-Rate-Limit von
  einer geteilten Runner-IP sieht genauso aus. Erst den Log lesen.
- **Geplante Workflows schlafen ein.** GitHub deaktiviert `schedule`-Trigger nach 60 Tagen ohne
  Repository-Aktivität. Nach der Abgabe läuft `digests.yml` also irgendwann stillschweigend
  nicht mehr.
- **Die SSE-Verteilung auf zwei Backend-Replicas ist nicht gemessen.** Sie folgt daraus, dass Caddy
  per `versions 1.1` je Stream eine eigene TCP-Verbindung öffnet und kube-proxy pro *Verbindung*
  auswählt. Das ist die richtige Konstruktion und der Grund, warum HTTP/2 hier ausdrücklich
  abgeschaltet ist — nachgezählt, welcher Stream auf welchem Pod landet, hat es aber niemand.
- **`chat.maxMessageLength` wirkt nur halb.** Der Server liest es aus der Umgebung, das Frontend hat
  die 500 in `index.html` und `app.js` fest stehen und kennt keinen Konfigurationsendpunkt. Wer den
  Server-Wert erhöht, bekommt ein Eingabefeld, das trotzdem bei 500 abschneidet.
- **`RedeTim.ChatClient` hat kein Testprojekt.** `--ensure-topic` ist gegen einen echten Broker
  vorgeführt (Abschnitt 5), aber nicht durch Unit-Tests abgesichert — dieselbe Form von Lücke, aus
  der der Readiness-Fehler entstanden ist.

### Was auf einem echten Cluster verifiziert wurde — und was nicht

kind läuft mit rootless Podman, die Punkte unten sind dort nachgefahren. **Verifiziert:** Build,
Tests, alle drei Images, der komplette Chat gegen ein echtes Redpanda, `--ensure-topic` gegen
denselben Broker (aus Quelltext und aus dem Image, im Neuanlage-, Bereits-vorhanden- und
Broker-nicht-erreichbar-Fall), und `helm lint` +
`helm template` + `kubeconform --strict` für alle Wertekombinationen inklusive externem Broker
und SASL.

Installation und anschließendes Upgrade auf einem echten Cluster: alle Pods erreichen `Ready`,
der Topic-Job legt `redetim-chat` an, und ein `POST /api/messages` trägt bis auf das Topic
(`202`, Key `general`, Offset 0). Alles mit `curl --cacert` gegen die Release-CA, nicht mit `-k`. Das Upgrade
behält dieselbe CA, statt sie stillschweigend zu rotieren.

Zwei Dinge zeigte erst der echte Cluster, beide behoben: mit podman gebaute Images lagen unter
`localhost/...`, wo das Chart sie nie findet — seit die Namen in `values.yaml`
registry-qualifiziert sind, entfällt das; und der Topic-Job konnte als `post-install`-Hook
prinzipiell nicht laufen, weil `--wait` auf ein Backend wartete, das ohne Topic nie `Ready`
wird.

**Nicht verifiziert** bleibt, was erst ein Scheduler mit mehreren Knoten zeigt: der `preStop`-Hook
des Frontends, `honor_labels`-Verhalten, Pod-Neustart-Resilienz, und dass `helm uninstall` das PVC
stehen lässt. Die Abnahmeliste in Abschnitt 12 ist der Plan dafür. Ebenso von Hand nachzuvollziehen
sind die Demo-Punkte 7 bis 9 aus Abschnitt 8: dass zwei Replicas einander vertreten, folgt aus dem
Offset-basierten Wiedereinstieg und ist auf Test- und Wire-Ebene abgesichert, aber nicht vorgeführt.

Zwei Punkte, die früher hier standen, sind erledigt: **SASL/TLS ist vorgeführt** (Abschnitt 5;
dabei kam der Readiness-Fehler heraus). Und ein PodDisruptionBudget „bremst" ein `kubectl drain`
auf einem Ein-Knoten-Cluster nicht — es **blockiert**: mit zwei Replicas und `maxUnavailable: 0`
gibt es keinen Zug, den der Scheduler machen könnte. Erwartetes Verhalten, aber nichts, was man
als Robustheit vorführt.

Vier Punkte aus einer Durchsicht gegen die zwölf Faktoren bleiben bewusst stehen:

- **Logzeilen lassen sich nicht zusammenführen.** `IncludeScopes` steht auf `false`, und die
  JSON-Zeilen tragen weder `TraceId` noch `SpanId`. Eine Backend-Zeile lässt sich damit nicht an
  die Caddy-Access-Log-Zeile desselben Requests hängen. Seit dem Rückbau der Telemetrie gibt es
  im Prozess ohnehin nichts mehr, das eine Trace-Id vergäbe — das wäre der erste Schritt, nicht
  das Einschalten des Feldes.
- **Nur das Backend hat einen HPA.** Ausgerechnet das Frontend nicht — dabei terminiert es jede
  Browser-Verbindung und öffnet wegen `versions 1.1` je SSE-Stream eine eigene TCP-Verbindung nach
  hinten. Seine Last hängt damit direkter an der Zahl gleichzeitiger Leser als die des Backends.
  `frontend.replicas` ist fest; ein `frontend.autoscaling` gibt es nicht. Für die Demo genügt das,
  gemeint ist es nicht als Aussage über die richtige Skalierungsachse.
- **Ein Pod, der den Broker verliert, bleibt bis zu 60 s im Service.** Die Readiness-Probe läuft
  alle 10 s mit `failureThreshold: 6`, und `BrokerReadiness` cached das negative Ergebnis
  zusätzlich 5 s. In diesem Fenster nimmt der Pod weiter `POST /api/messages` an, die dann
  fehlschlagen. Abschnitt 10 nennt nur die andere Richtung (ohne Broker wird ein startender Pod
  nie `Ready`); dies ist die Rückrichtung, und die Schwelle ist Absicht — kürzer würde ein
  einzelner langsamer Metadaten-Aufruf die Replica aus dem Service werfen.
- **`tls.publicHttpsPort` bedient zwei Adressaten mit einer Zahl.** Er ist als der Port
  dokumentiert, unter dem der *Browser* das Frontend erreicht (also der weitergeleitete Port aus
  `scripts/demo.sh`), und er ist zugleich das Ziel der `308`-Weiterleitung, die der Plaintext-Port
  im Cluster ausspricht. Solange beide Zahlen `8443` sind, fällt das nicht auf. Wer die Demo auf
  einen anderen lokalen Port legt und den Wert mitzieht, schickt jeden clusterinternen Aufrufer
  des `:8080`-Ports auf einen Port, den es dort nicht gibt. Im Release ruft niemand diesen Port
  intern auf, deshalb ist es heute latent.

---

## 14. Fehlerbehebung

**Caddy-Konfiguration prüfen — ohne Cluster und ohne Build.** `caddy validate` ist hier das
falsche Werkzeug, gleich doppelt: es beantwortet die Access-Log-Frage nicht (eine Konfiguration
ganz ohne Access-Log ist gültig), und es *provisioniert* die Konfiguration, öffnet also die
Zertifikatsdateien — die es außerhalb eines Pods nicht gibt. Es scheitert dann mit
`open /etc/redetim/tls/tls.crt: no such file or directory` an einem völlig intakten Caddyfile.
`caddy adapt` zeigt stattdessen, was der Server tatsächlich bekommt, und ist aus demselben Grund
auch der Check im Dockerfile:

```bash
podman run --rm -v "$PWD/src/RedeTim.Frontend/Caddyfile:/Caddyfile:ro" \
  docker.io/library/caddy:2.11.4-alpine@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648 \
  caddy adapt --config /Caddyfile --adapter caddyfile
```

Drei Dinge müssen in der Ausgabe stehen:

- unter `apps.http.servers.srv1` ein `logs`-Objekt (`{"default_logger_name":"log0"}`). Fehlt es,
  loggt der Server keinen einzigen Request — egal was im globalen Block steht. **`srv1`, nicht
  `srv0`:** `srv0` ist seit der TLS-Umstellung der Weiterleitungs-Listener auf `:8080`.
- unter `apps.tls.certificates.load_files` das Paar aus `/etc/redetim/tls`. Fehlt es, liefe der
  Server ohne Zertifikat an.
- im `reverse_proxy`-Handler ein `transport.tls.ca.pem_files` mit der CA. Fehlt **das**, prüft
  Caddy das Backend-Zertifikat gegen den System-Truststore — und der kennt die Release-CA nicht.

| Symptom | Ursache |
|---|---|
| `ImagePullBackOff` | GHCR-Paket noch privat, oder der Tag existiert dort nicht → Abschnitt 6 |
| Broker `CrashLoopBackOff`, „Argument parse error" | `command:` überschreibt den Entrypoint. Muss `[rpk, redpanda, start]` sein |
| Topic-Job läuft in den Timeout | Er wartet `TOPIC_WAIT_SECONDS` (180) auf Broker-**Metadaten**. Läuft das ab, ist der Broker nicht erreichbar — nicht der Job kaputt. Log des Job-Pods lesen: er nennt den letzten Fehler |
| Topic-Job endet sofort mit „no such host" | `REDPANDA_BOOTSTRAP_SERVERS` aus der ConfigMap zeigt ins Leere. Bei `redpanda.enabled=false` ist das `redpanda.external.bootstrapServers` |
| Broker startet, schreibt nicht | `fsGroup: 101` fehlt; frisches PVC gehört sonst root |
| Nachrichten kommen verzögert | Proxy puffert. Caddy tut das bei SSE nicht — nginx schon |
| `DllNotFoundException` beim Start | Backend-Image auf Alpine/Chiseled gebaut; librdkafka braucht glibc |
| Frontend-Logs zeigen keine Requests | Die `log`-Direktive steht im **globalen** Block. Dort konfiguriert sie Caddys Runtime-Logger und erzeugt keine einzige Request-Zeile. Access-Logs gibt es nur mit `log` **im Site-Block** — und `caddy validate` meldet die fehlende Zeile nicht |
| Browser: „Ihre Verbindung ist nicht privat" / `NET::ERR_CERT_AUTHORITY_INVALID` | Erwartet. Die CA gehört diesem Release, kein Truststore kennt sie. Einmal je Port akzeptieren oder die CA importieren → Abschnitt 7, „Zertifikate" |
| `curl: (60) unable to get local issuer certificate` | Ohne `--cacert` prüft curl gegen den System-Truststore. Die CA aus `redetim-ca` herausschreiben — `scripts/demo.sh` tut das beim Start |
| `curl: (60) … doesn't match target host name` | Der Aufruf nutzt einen Namen, der nicht im Zertifikat steht. Drin sind der Service-Name in allen vier Cluster-Schreibweisen, `localhost` und `127.0.0.1` — also `https://localhost:…` statt der Pod-IP |
| Frontend `CrashLoopBackOff`, `open /etc/redetim/tls/tls.crt: no such file` | Das TLS-Secret ist nicht gemountet oder heißt anders. `kubectl get secret redetim-frontend-tls` |
| Backend `CrashLoopBackOff`, Kestrel meldet ein fehlendes Zertifikat | Dasselbe für `redetim-backend-tls`, oder `ASPNETCORE_Kestrel__Certificates__Default__*` zeigt ins Leere |
| Caddy: `tls: failed to verify certificate: x509: certificate signed by unknown authority` | Der Upstream präsentiert ein Zertifikat einer anderen CA. Passiert nach einer halben Rotation: Secrets gelöscht, aber nicht alle Pods neu gestartet → `kubectl rollout restart` für beide |
| Nach `helm upgrade` starten alle Pods neu, obwohl nichts geändert wurde | Erwartet, aber nur **einmal** nach der allerersten Installation: `checksum/tls` hasht dort noch ein Wegwerf-Rendering, weil `lookup` beim Install noch nichts findet |

---

## Projektstruktur

```text
src/RedeTim.Contracts/    ChatMessage + Validierung + Wire-Format + KafkaSecurity (geteilt)
src/RedeTim.Backend/      ASP.NET Core: SSE, Kafka
src/RedeTim.Frontend/     Caddyfile + Vanilla-JS-Frontend (index.html, style.css, app.js, favicon.svg)
src/RedeTim.ChatClient/   Konsolenclient und Admin-Prozess (`--ensure-topic`, siehe Abschnitt 10)
tests/                      xUnit
deploy/helm/redetim/      Helm-Chart
deploy/releases/            generierte Release-Dateien (Image-Tags + Commit pro Build)
scripts/                    build-images.sh, validate-chart.sh, select-release.sh, check-repro.sh,
                            check-digests.sh, demo.sh, lib/common.sh
RedeTim-kafka-docker/     Redpanda für lokale Entwicklung ohne Kubernetes
```
