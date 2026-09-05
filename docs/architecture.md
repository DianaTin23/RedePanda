# Architektur

RedeTim besteht aus zwei eigenständig geschriebenen und eigenständig ausgelieferten
Anwendungen, die **ausschließlich über Redpanda** miteinander reden. Das Frontend kennt Kafka
nicht, das Backend kennt Prometheus nicht. Beides ist Absicht, und beides ist in der Demo
nachweisbar.

Die Strecken- und Protokolltabelle steht in README Abschnitt 3. Hier steht, warum der Schnitt
so verläuft.

## Der Schnitt

```text
Browser ──HTTPS──▶ Caddy (Frontend-Pod) ──proxy /api──▶ Backend-Pod ──Kafka──▶ Redpanda
   ▲                 :8443                 HTTPS :8443     │  │                (StatefulSet)
   └────────── SSE-Stream (/api/stream) ◀──────────────────┘  │
                                                              │ OTLP/gRPC über TLS :4317 (push)
                                                              ▼
                                                     OTel-Collector-Pod
                                                              │ HTTPS :8889 /metrics
                                                              ▼ (scrape)
                                                       Prometheus-Pod :9090 (HTTPS)
```

Zwei Trennungen tragen den Entwurf, und beide sind so gebaut, dass man sie *zeigen* kann statt
sie nur zu behaupten:

**Das Frontend spricht kein Kafka.** Es besteht aus vier statischen Dateien, die Caddy
ausliefert, und redet nur mit `/api/...`. Im Netzwerk-Tab des Browsers taucht nichts anderes
auf. Kein npm, kein CDN, keine Webfonts — das funktioniert damit auch in einem Cluster ohne
Internetzugang. Mehr dazu in [frontend.md](frontend.md).

**Das Backend spricht kein Prometheus.** Es pusht über OTLP und hat gar keinen
`/metrics`-Endpunkt; ein `curl` darauf gibt 404. Es kennt nur einen OTLP-Endpunkt aus einer
Umgebungsvariable. Mehr dazu in [observability.md](observability.md).

Der Metrikpfad zeigt bewusst *vom Backend weg*. Das ist dieselbe 12-Factor-Argumentation wie
beim Broker: eine angehängte Ressource, austauschbar ohne Codeänderung.

Jede HTTP-Strecke im Release ist TLS, und jeder Client prüft sein Gegenüber gegen die CA, die
das Chart bei der ersten Installation selbst ausstellt. `insecure_skip_verify` steht nirgends.
Die zwei bewussten Ausnahmen stehen in README Abschnitt 14 und in
[observability.md](observability.md#tls-und-die-eine-ausnahme).

## Warum SSE und kein SignalR

SignalR bräuchte ein WebSocket-Upgrade durch den Proxy, Sticky Sessions und ab der zweiten
Replica ein Backplane. Für einen reinen Server→Browser-Broadcast genügt SSE. Es kommt durch
jeden Proxy und ohne Client-Bibliothek aus.

Auch bei mehreren Replicas braucht es keins von beidem. Die `id` jedes Datenframes ist der
Kafka-Offset, und der gehört dem Broker, nicht einem einzelnen Pod. Ein Browser, dessen
Verbindung abreißt und der auf einer *anderen* Replica wieder aufsetzt, schickt denselben
`Last-Event-ID` mit und bekommt genau das, was er verpasst hat.

Das Backplane ist Redpanda selbst. `sessionAffinity` am Service wäre deshalb nicht nur
überflüssig, sondern schädlich — es verlängerte einen Rollout künstlich.

Details zum Streamverhalten: [streaming.md](streaming.md).

## Ein Topic für alle Räume

Der Raum steht als Feld *und* als Kafka-Key in der Nachricht.

Ein Topic bedeutet einen einzigen Init-Job. Der Key sichert die Reihenfolge je Raum, falls das
Topic je mehr Partitionen bekommt. Gefiltert wird serverseitig im Backend.

Die Aussage betrifft *Räume*, nicht "die App hat insgesamt nur ein Topic": es gibt keinen
Topic pro Raum, weil ein Raum kein fester Wert ist, sondern aus einem Query-String kommt. Ein
zweites, strukturell anderes Topic hält davon unabhängig den Präsenz-Stand (wer welchen
Nickname in welchem Raum gerade hält) — log-komprimiert statt angehängt, weil dort der
*aktuelle* Zustand zählt und nicht die Historie. Details in [kafka.md](kafka.md#presence-topic).

## Kein `GET /api/history`

Es gibt bewusst keinen zweiten Endpunkt für den Verlauf. `GET /api/stream` schickt ihn als
erste Frames, bevor der Live-Betrieb beginnt.

Ein separater Aufruf hätte eine Lücke zwischen „Verlauf geladen" und „Stream offen" — und
genau dort ginge eine Nachricht verloren.

## Die geteilten Typen

`RedeTim.Contracts` enthält das, worüber sich Backend und Konsolenclient einig sein müssen.
Es ist absichtlich klein.

### `ChatMessage`

Das Format auf dem Topic. Der Kafka-Record-Key ist `Room`.

Grenzen: Raum 64 Zeichen, Nickname 32, Text `MAX_MESSAGE_LENGTH` (Vorgabe 500).

Die Längenprüfung läuft **nach** dem Trimmen, damit man das Limit nicht mit angehängten
Leerzeichen auslösen kann.

### `WireFormat`

Die einzige Stelle, an der das Payload-Format definiert ist — für die Chat-Nachricht wie für
den Präsenz-Record. Weder Backend noch Konsolenclient dürfen `JsonSerializer` mit eigenen
Optionen aufrufen — serialisierten sie unterschiedlich, verstünden sie einander still nicht
mehr. Genau das war hier schon der Fall: die Präsenz hatte eine zweite, zeichengleiche Kopie
derselben Optionen, und nichts hätte gemeldet, wenn eine der beiden abgewichen wäre.

`Deserialize` gibt bei allem Unlesbaren `null` zurück. Ein fremder oder beschädigter Record
darf die Consume-Schleife nicht beenden.

### `ChatRecord`

Nachricht plus Kafka-Offset, und zwar **nur im Backend**. Der Offset gelangt nie in die
Payload; er wird ausschließlich in das SSE-`id`-Feld geschrieben.

`ChatMessage` ist das Format, das auch der Konsolenclient parst. Es hier zu erweitern hätte das
für einen Wert kaputtgemacht, den der Browser braucht und kein Consumer des Topics.

Weil der Raum der Record-Key ist, landen alle Nachrichten eines Raums auf derselben Partition.
Die Offsets, die ein SSE-Stream sieht, steigen deshalb streng monoton — genau die Eigenschaft,
die `Last-Event-ID` braucht, um eine abgerissene Verbindung ohne Wiederholung und ohne
Auslassung fortzusetzen.

### `SendMessageRequest`

Der Body von `POST /api/messages`. Er hat **kein Zeitstempelfeld**.

Der Server stempelt jede Nachricht aus seiner eigenen Uhr. Ein Client kann eine Nachricht also
weder vor- noch zurückdatieren, selbst wenn er es versucht — der Wert hat schlicht nichts, woran
er binden könnte. In `ChatMessage.TryCreate` ist der Zeitstempel deshalb ein Parameter und
keine Client-Eingabe.

## Manuelle Kopplungen

CI prüft Tests, Lock-Dateien und das Chart (README Abschnitt 13), die folgenden Kopplungen aber
nicht — sie hängen an Werten in anderen Dateien, ohne dass irgendetwas sie vergleicht. An diesen
Stellen steht im Code je eine Zeile, die darauf hinweist; sie sind die einzige Kontrolle.

| Ort | hängt an | Was bei Drift passiert |
|---|---|---|
| `wwwroot/app.js` (Textlängengrenze) | `ChatMessage.DefaultMaxTextLength` | Der Client lässt mehr zu, als das Backend annimmt; der Nutzer sieht ein 400 statt einer Warnung im Eingabefeld. |
| `RedeTim-kafka-docker/docker-compose.yml` (und `make-tls.sh`) | `redpanda.image` in `values.yaml` | Die lokale Broker-Version weicht von der im Cluster ab — Dev/Prod-Parität nur noch auf dem Papier. |
| `ChatMetrics` (Instrumentnamen) | Suffixregeln des Prometheus-Exporters | Namen kommen mit falschem oder doppeltem Suffix in Prometheus an, die PromQL-Beispiele laufen leer. |
| `Directory.Build.props` | XML erlaubt kein `--` im Kommentar | MSBuild meldet ein leeres `TargetFramework` aus einer völlig anderen Datei. |
| `backend.yaml` (`replicas`) | ob ein HPA das Feld besitzt | Helm und Autoscaler überschreiben sich gegenseitig, die Pod-Zahl pendelt. `scripts/validate-chart.sh` rendert beide Varianten und vergleicht gegen `values.yaml`. |
| Digest-Pins in `values.yaml` und den Dockerfiles | den Tags upstream | `scripts/check-digests.sh` meldet es, wöchentlich aus `digests.yml`. Umgeschrieben wird nichts. |

Die beiden Skripte `check-digests.sh` und `check-repro.sh` sind der automatisierbare Teil davon
und laufen inzwischen von selbst: `check-repro.sh` bei jedem Push und PR, `check-digests.sh`
wöchentlich. Die Zeilen der Tabelle darüber bleiben unbewacht — deshalb stehen sie hier.

## Prozesse

Es gibt vier Admin-Prozesse, alle als Kubernetes-Job:

- Topic anlegen (`RedeTim.ChatClient --ensure-topic`)
- die drei weiteren Aufgaben im `admin-job`

Der Topic-Job ist **kein** Helm-Hook. Ein `post-install`-Hook wartete auf ein Backend, das
ohne Topic nie `Ready` wird — ein Deadlock. Details in [deployment.md](deployment.md).

Kein Pod hält dauerhaften lokalen Zustand. Der Verlaufspuffer ist eine Projektion des Topics,
die jeder Pod beim Start neu aufbaut; SSE-Verbindungen sind bewusst prozesslokal.
