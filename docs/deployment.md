# Deployment

Helm ist der Installationsweg, und zwar der einzige. Die Befehle stehen in README Abschnitt 7,
die Schalter in der Tabelle in Abschnitt 9. Hier steht, warum das Chart so gebaut ist.

Warum es kein gerendertes `rendered.yaml` mehr gibt, steht in
[build.md](build.md#warum-es-kein-gerendertes-manifest-mehr-gibt).

## Ein Release ist ein Artefakt, kein Zustand

Das Chart **weigert sich zu rendern**, wenn kein Release-File übergeben wurde. Der Image-Tag hat
absichtlich keinen Vorgabewert.

Jeder plausible Vorgabewert wäre ein veränderlicher Name. Einen zu deployen hieße, ein nicht
identifizierbares Image in den Cluster zu stellen und das nächste `helm rollback` zu einer
Leeroperation zu machen. Hier zu scheitern kostet einen Befehl; im Cluster zu scheitern kostet
eine Stunde.

Dasselbe Muster gilt für `redpanda.external.bootstrapServers`, wenn kein Broker mitdeployt wird:
harter Fehler statt Vorgabewert. Die Alternative wäre ein Backend, das hochkommt und jede
Verbindung gegen einen Service scheitern lässt, den niemand deployt hat.

> **`helm lint` fängt das nicht.** Helm 4 stuft ein `fail` im Template auf INFO herab und meldet
> trotzdem „0 chart(s) failed". Nur `helm template` bricht wirklich ab. README Abschnitt 13
> nennt den Befehl.

Die lokal gebauten Images tragen **keinen** Digest, anders als die Fremdimages. Sie werden nie
aus einer Registry gezogen — ein Digest schützte also gegen etwas, das ohnehin nicht passiert,
und bräche zusätzlich `imagePullPolicy: IfNotPresent`, das über den Tag vergleicht. Was bei den
Fremdimages der Digest leistet, leistet hier der Tag: `build-images.sh` leitet ihn aus
`appVersion` und dem Commit ab und verwendet ihn nie erneut.

`imagePullPolicy` ist `IfNotPresent` und nicht `Never`. `Never` scheiterte auf Docker Desktop
rundheraus mit `ErrImageNeverPull`, obwohl das Image bereits vorliegt und gar nichts gezogen
würde.

Das Label `app.kubernetes.io/version` trägt die **Release**-Version, nicht `appVersion` des
Charts. Die Frage, die dieses Label im Cluster beantworten muss, lautet „welcher Build läuft
hier", und `appVersion` ist auf jeder Revision dieselbe Zeichenkette. Es zu ändern ist
gefahrlos, weil es kein Selektor ist — `redetim.selectorLabels` ist bewusst getrennt und
enthält nur unveränderliche Identität, damit der Selektor eines Deployments nie wandert.

## TLS

Das Chart stellt bei der ersten Installation eine eigene CA aus und signiert damit vier
Server-Zertifikate. Es gibt **keinen Ausschalter** dafür.

Das ist Absicht: Eine Aus-Stellung, die niemand vorführt, ist ein ungetesteter Codepfad, und die
Templates trügen dann eine zweite Variante jedes Ports, jeder Probe und jedes Mounts.

Der private Schlüssel der CA bleibt im Cluster. Ein späteres Upgrade braucht ihn, um ein
Zertifikat für eine Komponente zu signieren, die erst dann eingeschaltet wird — etwa
`otelCollector.enabled`.

`ca.crt` liegt neben jedem Schlüsselpaar, damit ein Pod, der zugleich Server und Client ist —
das Frontend als Proxy zum Backend, Prometheus beim Scrapen des Collectors — mit genau einem
Mount auskommt. Aus demselben Grund ist der Mountpfad für alle Komponenten derselbe: Ein Pfad je
Komponente wäre nur eine weitere Sache, die man in einer Probe oder einer Konfigurationsdatei
falsch machen kann.

Zertifikate werden über Upgrades hinweg wiederverwendet und nie still rotiert. Das Rotieren ist
ein Löschen-und-Upgraden; README Abschnitt 7 hat die Befehle.

`certValidityDays: 825` ist die längste Laufzeit, die Safari und Chrome für ein
Leaf-Zertifikat akzeptieren. Für eine private CA ist das belanglos — die Browser erzwingen es
dort nicht — und steht trotzdem so da, damit die Zahl nicht später verteidigt werden muss.

## Das Backend-Deployment

### Zwei Replicas, und warum das echte Arbeit ist

Jeder Pod konsumiert unter eigener GroupId und sieht damit jede Nachricht. Die SSE-`id` ist der
Kafka-Offset, der dem Broker gehört und keinem einzelnen Pod. Ein Browser, der auf einer
*anderen* Replica wieder aufsetzt, schickt dieselbe `Last-Event-ID` und bekommt genau das, was
er verpasst hat.

Deshalb braucht der Service weder Sticky Sessions noch `sessionAffinity`. In der Demo lässt sich
ein Pod löschen, und die Fenster am anderen merken nichts.

Das Frontend läuft aus demselben Grund zweifach, mit mehr Nachdruck: **Jeder** SSE-Stream läuft
durch dieses Deployment. Mit einem einzigen Caddy-Pod wäre die Redundanz des Backends nicht
beobachtbar, weil ein einziger geräumter Node trotzdem jeden Browser trennte. Caddy hält keinen
Zustand, die zweite Replica kostet 10m CPU.

### Wem `replicas` gehört

Ist ein HPA aktiv, lässt das Deployment das Feld **weg**. Sonst schriebe jedes `helm upgrade`
die Zahl aus `values.yaml` zurück, der Autoscaler korrigierte sie sofort wieder, und die Pod-Zahl
pendelte zwischen beiden.

### Rollout

`maxUnavailable: 0`. Bereitschaft heißt hier „das Topic bis ans Ende nachgelesen", ein neuer Pod
tritt dem Service also erst bei, wenn er einen vollständigen Raum ausliefern kann — und der alte
darf erst gehen, wenn das passiert ist.

`maxSurge: 1` als absolute Zahl statt als Prozentsatz: Es rollt einen Pod nach dem anderen,
gleichgültig was der Autoscaler mit der Replica-Zahl gemacht hat. Das ist die vorsichtige Wahl,
wenn Bereitschaft an einem Broker-Roundtrip hängt.

`minReadySeconds: 5`. Bereitschaft ist nach dem Replay eine Einwegsperre, das schützt also nur
gegen eine einzelne erfolgreiche Probe gegen einen Pod, der den Broker gleich wieder verliert.
Kostet 5 s je Pod.

Zwei `checksum`-Annotationen rollen die Pods, wenn sich Konfiguration oder Zertifikate ändern.
Ohne die erste liefe nach einem `helm upgrade`, das nur die ConfigMap ändert, weiterhin der alte
Wert. Ohne die zweite lieferten die Pods das alte Zertifikat aus, während ihre Gegenstellen
bereits nur der neuen CA vertrauen.

> Bei der allerersten Installation hasht die Zertifikats-Annotation ein Wegwerf-Rendering —
> `lookup` findet noch kein Secret. Das erste Upgrade danach rollt die Pods deshalb einmal.

`topologySpreadConstraints` bevorzugt eine Replica je Node und verweigert die Einplanung dafür
nie. Eine harte Bedingung (`DoNotSchedule` oder Anti-Affinität `requiredDuringScheduling`) ließe
die zweite Replica auf den Ein-Node-Clustern, auf die dieses Chart zielt, für immer `Pending` —
ein schlechteres Ergebnis als gar keine Verteilung. Auf einem Node nachweislich wirkungslos, auf
mehreren richtig; README Abschnitt 14 sagt, welchen Fall die Demo tatsächlich vorführt.

Es gibt **keine** `prometheus.io/scrape`-Annotationen. Das Backend pusht über OTLP und hat
keinen `/metrics`-Endpunkt.

### Probes und Shutdown

Der kubelet spricht bei `scheme: HTTPS` zwar TLS, prüft das Zertifikat aber bewusst nicht — er
hat keine Möglichkeit, die CA dieses Releases zu bekommen. Das ist dokumentiertes Verhalten und
in Ordnung: Eine Probe fragt „lebt dieser Prozess", nicht „ist er, wer er behauptet zu sein".

`/health/ready` antwortet 503, bis Broker-Metadaten abrufbar sind; der Pod fällt damit aus den
Service-Endpoints, solange Redpanda unerreichbar ist. Das Ergebnis wird 5 s gecached — Details
in [kafka.md](kafka.md#bereitschaftsprüfung-gegen-den-broker).

Der `preStop`-Hook schläft 5 s. Endpunkt-Entfernung und SIGTERM werden parallel abgeschickt, und
kube-proxy braucht einen Moment, um keine neuen Verbindungen mehr hierher zu lenken. Zuerst zu
schlafen heißt, dass kein Browser an einen Pod gereicht wird, der schon auf dem Weg hinaus ist.

Es ist ein `exec`-Hook und ausdrücklich nicht das neuere `lifecycle.preStop.sleep`: Dieses Feld
ist erst ab einer neueren Kubernetes-Version verfügbar. Aufgerufen wird `/bin/sleep`, also
coreutils' `sleep` und keine Shell — das Debian-basierte aspnet-Basisimage hat es.

`terminationGracePeriodSeconds: 45` ist länger als die 25 s Shutdown-Timeout, die sich die
Anwendung selbst setzt, damit der Consumer seine Group verlassen und der Producer flushen kann.
Die vollständige Rechnung steht in [kafka.md](kafka.md#herunterfahren).

Das Frontend kommt mit 30 s aus: Dort muss weder eine Consumer-Group verlassen noch ein Producer
geflusht werden.

## Autoscaling

Aus per Vorgabe, und das ist eine Entscheidung und kein Versäumnis. Ein HPA braucht
metrics-server, den weder kind noch Docker Desktop mitbringen. Ohne ihn existiert das Objekt,
liest für immer `<unknown>/70%` und skaliert nie — was kaputter aussieht als gar kein
Autoscaler. README Abschnitt 7 hat die zwei Befehle zum Einschalten.

**Skaliert wird über CPU**, weil das Backend per Entwurf keinen `/metrics`-Endpunkt hat: Die
einzige Metrik, an die ein einfacher HPA herankommt, ist die des kubelet. Die interessante Zahl
wäre `redetim_active_connections` je Pod, aber sie zu lesen bräuchte prometheus-adapter oder
KEDA — eine ganze Zusatzkomponente, um eine Demo zu autoskalieren. Der Weg bleibt offen.

Die Zielauslastung bezieht sich auf den CPU-**Request**, nicht auf das Limit und nicht auf den
Node. Deshalb steht der Request auf 100m und nicht auf den 50m, die für eine Ein-Pod-Demo
gereicht hätten: Bei 50m sind 70 % gleich 35m, und daran streift ein untätiger
ASP.NET-Core-Pod mit GC und einem 5-Sekunden-Metrikexport bereits — der HPA säße ab der ersten
Sekunde bei `maxReplicas`.

**Es gibt weiterhin kein CPU-Limit**, wie überall sonst in diesem Chart. Es änderte an der
Rechnung des HPA nichts (gemessen wird gegen den Request), und CFS-Throttling deckelte genau das
Signal, das der HPA liest: Ein überlasteter Pod meldete sich dann als bloß beschäftigt.

`minReplicas: 2` — nie unter die Basislinie, für die PodDisruptionBudget und Rollout-Strategie
geschrieben sind. Bei einer Replica hat `maxSurge` nichts, wogegen es aufstocken könnte, und
`maxUnavailable: 0` ist nicht einzuhalten.

`maxReplicas: 4` ist Arithmetik, keine Schätzung: 4 × 100m + Redpanda 200m + Collector 50m +
Prometheus 50m + 2 × Frontend 10m = 720m. Passt auf einen Ein-CPU-Node.

Hoch schnell, runter langsam. Für das Hochskalieren bleibt bewusst die Kubernetes-Vorgabe (kein
Stabilisierungsfenster) — eine Vorgabe auszuschreiben wäre Rauschen, nur die Asymmetrie ist
festhaltenswert. Das Herunterskalieren steht auf 60 s statt der vorgegebenen 300 s: Letztere sind
für den Produktivbetrieb richtig und länger als die ganze Demo.

Das ist dieselbe Begründung wie beim Metrik-Exportintervall (5 s statt 60 s) und beim
Scrape-Intervall (5 s statt 15 s): **Eine Zahl, die niemand sich bewegen sieht, beweist nichts.**

## ConfigMap

Alle Einstellungen der Anwendung stehen unter schlichten Namen (`REDPANDA_TOPIC`,
`LOG_LEVEL`, …), wie die Aufgabenstellung es verlangt. Deshalb liest `BackendOptions` sie
ausdrücklich, statt sich auf ASP.NETs Auto-Binding zu verlassen, das `Section__Key` erwartet.

Es gibt genau zwei Ausnahmen von der Regel „schlichte Namen":

- **`OTEL_*`** — von OpenTelemetry spezifiziert und vom SDK selbst gelesen. Sie im Code noch
  einmal zu lesen erzeugte eine zweite Wahrheitsquelle.
- **`ASPNETCORE_Kestrel__Certificates__Default__*`** — Framework-Konfiguration, die ASP.NET
  selbst besitzt.

**Zugangsdaten stehen nie in der ConfigMap.** Protokoll und Mechanismus sind Konfiguration,
Benutzername und Passwort sind es nicht. Sie kommen aus einem Secret, das man selbst anlegt, und
werden über einen Helper in beide Pod-Templates gerendert, die Kafka sprechen.
`redpanda.auth.existingSecret` ist bewusst ein Verweis und kein Wert: Werte in `values.yaml`
landen in `helm get values`, im Shell-Verlauf und in jedem Repository, das die Datei enthält.

`LOG_LEVEL` wird vom Backend gelesen, und **nur** von ihm. Es stand einmal in einer Aufzählung
von Variablen, die der Topic-Job liest, was nie stimmte: Die Admin-Prozesse schreiben schlichte
Zeilen nach stdout und haben keinen Logger, dessen Level man setzen könnte. Wer den Wert
hochdrehte, um einen scheiternden Topic-Job zu untersuchen, wartete auf Ausgabe, die nicht
kommen kann.

`backend.extraEnv` reicht zusätzliche Variablen durch. Sie stehen im Container **hinter** der
ConfigMap, und Kubernetes lässt den späteren Eintrag gewinnen — der Block kann also auch
überschreiben:

```sh
helm upgrade ... --set-json 'backend.extraEnv=[{"name":"LOG_LEVEL","value":"Debug"}]'
```

Vorher gab es im ganzen Chart keinen einzigen Durchreicher für Umgebungsvariablen, und jede neue
Variable brauchte eine Template-Änderung. Für ein Chart, dessen Thema „Konfiguration in der
Umgebung" ist, war das die falsche Form.

## Der Topic-Job

**Er ist ausdrücklich kein Helm-Hook.** Als `post-install`-Hook könnte er nie laufen: Helm führt
`post-install`-Hooks erst aus, nachdem `--wait` das Release als bereit meldet, und das Backend
wird nicht bereit, bevor es das Topic gelesen hat, das dieser Job anlegt. Ein Deadlock ohne
Ausweg — die Installation stünde bis zum Timeout auf `pending-install` und hätte überhaupt
keinen Job erzeugt.

Als gewöhnliche Ressource wird er zusammen mit Broker und Deployments angewandt, und `--wait`
wartet wie auf alles andere darauf, dass er `Complete` erreicht. Die Reihenfolge löst sich damit
von selbst: Der Job pollt auf Broker-Metadaten, während das Backend sein `Subscribe`
wiederholt, und beide beruhigen sich, sobald das Topic existiert. Ein `helm uninstall` entfernt
ihn außerdem, was bei einem Hook nicht der Fall wäre.

Der Name trägt die Release-Revision. Das Pod-Template eines Jobs ist unveränderlich, ein fester
Name ließe also jedes `helm upgrade` an „field is immutable" scheitern. Ein neuer Name je
Revision umgeht das, und Helm räumt den Job der Vorrevision ab, weil er im abgeglichenen
Manifest nicht mehr vorkommt.

Er läuft im **eigenen Image der Anwendung**, mit `--ensure-topic`. Früher war das `rpk` im
Redpanda-Image, gesteuert von einem dreißigzeiligen Shell-Skript — eine zweite Codebasis und
eine zweite Konfigurationsquelle für eine Aufgabe, die zu dieser Anwendung gehört. 12-Factor XII
verlangt einen Admin-Prozess, der mit der Anwendung ausgeliefert wird und läuft. Der Preis dafür
ist ein Image, das dasselbe Skript ohnehin schon baut.

Er liest **dieselbe ConfigMap wie das Backend**, vollständig und unverändert. Nichts über Broker
oder Topic wird hier wiederholt, die beiden können also nicht auseinanderlaufen.

`brokerWaitSeconds: 180` ist großzügig, weil der Job beim Install gegen einen gerade erst
startenden Broker läuft. Zusammen mit `backoffLimit: 10` ergibt das die Obergrenze, bis zu der
ein fehlender Broker den Job beschäftigt: 10 × dieser Wert. Wer das `--timeout` von
`helm upgrade` kürzer setzt, sieht die Installation scheitern, während der Job noch berechtigt
weiterläuft.

Gewartet wird auf **Metadaten**, nicht auf `rpk cluster health`. Letzteres braucht die Admin-API
auf 9644 — einen Port, den ein verwalteter Broker nicht offenlegen muss.

Der Wert stand einmal fest im Template, während README Abschnitt 9 `TOPIC_WAIT_SECONDS` als
Einstellung führte. Gegen einen fremden, langsam startenden Broker war das eine Änderung an
einem Template statt an einer Wertedatei.

## Austauschbare Backing Services

Zwei Schalter machen aus einer Behauptung einen Nachweis:

**`redpanda.enabled: false`** deployt keinen Broker; die Anwendung zeigt dann auf
`redpanda.external.bootstrapServers`. Ohne diesen Schalter bliebe die 12-Factor-Aussage in README
Abschnitt 11 unbelegt.

**`otelCollector.enabled: false`** plus `otelCollector.external.endpoint` exportiert an einen
vorhandenen Collector. Die Anwendung konnte das immer — sie liest
`OTEL_EXPORTER_OTLP_ENDPOINT` —, das Chart nicht: Der einzige Weg zu einem fremden Collector war
eine Änderung an `templates/configmap.yaml`.

Steht dort kein Endpunkt, wird das SDK per `OTEL_SDK_DISABLED` abgeschaltet, statt einen
Endpunkt anzufragen, den es nicht gibt.

Prometheus muss dabei ebenfalls aus: Das mitgelieferte Prometheus kennt als Scrape-Ziel nur den
mitgelieferten Collector. Das Chart sagt das beim Rendern.

`otelCollector.external.caSecret` gehört dazu. Ohne diesen Schlüssel galt der Austausch des
Telemetrie-Backends nur unter einer Bedingung, die nirgends stand: Der Init-Container, der die
CA in den Trust Store faltet, hing an `otelCollector.enabled`. Ein fremder Collector hinter
privater CA war damit nicht erreichbar — ohne Fehlermeldung, die das erklärt hätte.

## Helpers

`redetim.fullname` kollabiert beim üblichen Release-Namen `redetim` zu `redetim`. Das ist
es, was die Service-Namen in der README kurz hält: `redetim-backend`,
`redetim-otel-collector`.

**Der Service-Name des Brokers ist bewusst nicht release-qualifiziert.** Er steht in
`--advertise-kafka-addr` des Brokers und im Bootstrap-Vorgabewert jedes Clients; ein kurzer,
stabiler Name hält beides lesbar und deckungsgleich mit der Dokumentation.

`redetim.securityProtocol` normalisiert die Schreibweise, indem es `_` und `-` entfernt —
genauso wie `KafkaSecurity` im Code. Alles Unbekannte ist ein **harter Fehler**, und das ist der
eigentliche Grund für den Helper. Die vorige Fassung verglich gegen zwei Zeichenketten und
behandelte alles, was nicht passte, als „kein SASL" — ein Tippfehler im Protokollnamen führte
also stillschweigend zu einem Deployment ohne Zugangsdaten.

`redetim.saslEnv` ist ein Helper und keine achtfach duplizierte Zeile, weil beide
Pod-Templates, die Kafka sprechen, ihn rendern.

`redetim.releaseAnnotations` lässt leere Werte weg, statt sie blank zu rendern, damit
`kubectl describe` still bleibt, wenn das Chart ohne Release-File gerendert wird — etwa bei
`helm lint`.

## Der Broker

Das Image kommt von **docker.io und nicht von docker.redpanda.com**. Letzteres ist ein
Pull-Through-Proxy vor demselben Repository und drosselt anonyme Manifest-Zugriffe so hart, dass
der Digest sich nicht verlässlich auflösen oder nachprüfen lässt. Auf den Ursprung zu zeigen
hält den Pin überprüfbar. `RedeTim-kafka-docker/docker-compose.yml` wurde mitgezogen, damit
lokale Entwicklung und Chart sich weiter über ein Image einig sind — `check-digests.sh` prüft
genau das, siehe [build.md](build.md#broker-parität).

`smp` und `memory` sind ausdrücklich gesetzt: Seastar bemisst sich in einem Container sonst am
ganzen Node.

Redpanda läuft im `dev-container`-Modus mit einem einzelnen Broker. Das ist eine bewusste
Einschränkung der Dev/Prod-Parität und in README Abschnitt 14 festgehalten.

## Der Collector

**Core-Distribution, nicht contrib.** Core enthält den Prometheus-Exporter und die
`health_check`-Extension bereits. `otelcol-k8s` wäre falsch — es hat überhaupt keinen
Prometheus-Exporter.

Die Konfiguration selbst ist in [observability.md](observability.md#collector-konfiguration)
beschrieben.
