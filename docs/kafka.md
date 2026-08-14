# Kafka: Producer, Consumer, Offsets

Das Topic ist nicht nur der Transportweg zwischen den Backend-Pods, es *ist* der Chatverlauf.
Alles andere — der Verlaufspuffer im Speicher, die SSE-Verbindungen — ist eine Projektion
davon, die jeder Pod beim Start neu aufbaut.

Dieses Dokument erklärt, wie gelesen und geschrieben wird. Wie die gelesenen Nachrichten in
den Browser kommen, steht in [streaming.md](streaming.md).

## Eine Consumer-Group je Pod

Jeder Pod erfindet seine eigene GroupId: `redepanda-backend-<POD_NAME>`. Das ist die
Entscheidung, auf der die gesamte Skalierbarkeit ruht.

Der Normalfall bei Kafka ist umgekehrt: Pods teilen sich eine Group und damit die Partitionen.
Hier wäre das falsch. Eine SSE-Verbindung gehört genau einem Pod. Bekäme ein Pod nur einen
Teil der Nachrichten, sähen die daran hängenden Browser auch nur einen Teil des Gesprächs.
Bei einer einzigen Partition wäre es noch drastischer: Kafka gäbe sie *einem* Pod, und jeder
Browser an allen anderen Pods säße in einem Raum, der sich nie aktualisiert.

Mit einer eigenen Group je Pod bekommt jeder Pod jede Nachricht. Das ist Fan-out, kein
Lastausgleich — die Leselast steigt mit der Replica-Zahl. Für einen Chat ist das der richtige
Tausch.

Der Preis: `POD_NAME` muss stimmen. Im Cluster kommt der Name aus einem `fieldRef` auf
`metadata.name`; den garantiert der API-Server als eindeutig im Namespace. Fehlt die Variable,
obwohl der Prozess in Kubernetes läuft, **wirft** `ResolvePodName` statt sich einen Namen
auszudenken. Ein Ersatzname wäre hier das Schlimmste: der Pod liefe, träte der Group bei und
brächte den Fan-out still zum Erliegen. Erkannt wird Kubernetes an
`KUBERNETES_SERVICE_HOST`, die der kubelet in jedem Pod setzt und sonst niemand.

Lokal ist dieselbe Kollision erreichbar, indem man das Backend zweimal auf einer Maschine
startet — genau so probiert man den Fan-out aus. Dort trennt die Prozess-ID die beiden, und
der Name bleibt in `rpk group list` lesbar.

Weil jede Pod-Inkarnation eine neue GroupId erfindet, ist `EnableAutoCommit` aus. Committete
Offsets hinterließen für die volle Retention Offset-Records im Broker, die nie wieder jemand
liest.

## Wie weit ein startender Pod zurückliest

Ein Pod muss den Verlauf kennen, bevor er Browser bedienen darf. Früher las dafür jede Replica
das *gesamte* Topic. Startzeit und Leselast am Broker wuchsen damit sowohl mit dem Topic als
auch mit der Replica-Zahl — am schlimmsten also genau dann, wenn ein Autoscaler Pods
hinzufügt, weil die vorhandenen schon unter Last stehen.

`CHAT_REPLAY_RECORDS` (Vorgabe 2000) begrenzt das **je Partition**. Der
`PartitionsAssignedHandler` fragt für jede zugewiesene Partition die Watermarks ab und startet
bei `max(low, high - replayRecords)`.

Das `max` mit `low` ist der wichtige Teil. Ein Offset unterhalb der Low-Watermark ist kein
Fehler, den librdkafka meldet — die Bibliothek fällt still auf `auto.offset.reset` zurück. Der
Pod läse dann wieder das ganze Topic und sähe von außen genauso aus, als hätte er ein Fenster
angefordert. Deshalb steckt die Rechnung in `StartOffsetFor`, wo sie ohne Broker testbar ist.

Schlägt die Watermark-Abfrage fehl (Timeout 10 s), liest der Pod die Partition vollständig.
Das ist das alte, langsamere Verhalten, nicht das falsche — aber es wird geloggt. Ein Pod, der
still den langsamen Weg nimmt, ist die Art von Startzeit-Regression, die niemand erklären kann.

`CHAT_REPLAY_RECORDS=0` bedeutet weiterhin „alles, was der Broker noch hat".

### Warum das eine andere Zahl ist als `CHAT_HISTORY_SIZE`

Die beiden zählen Verschiedenes, und sie ineinander umzurechnen würde den Unterschied
verstecken:

- `CHAT_HISTORY_SIZE` gilt **je Raum** und begrenzt den Speicher.
- `CHAT_REPLAY_RECORDS` gilt **je Partition** und begrenzt die Startzeit.

Ein Pod, der genau `CHAT_HISTORY_SIZE` Records zurückläse, füllte jeden Raum nur unvollständig,
sobald mehr als ein Raum aktiv ist. Am Namen der Einstellung hätte das niemand bemerkt.

## Wann ein Pod bereit ist

`EnablePartitionEof = true` macht aus „im Moment keine weiteren Records" ein echtes
`Consume`-Ergebnis. Ohne das gäbe es keine Möglichkeit, das Nachladen vom Live-Betrieb zu
unterscheiden, und der Pod könnte sich nie zum richtigen Zeitpunkt bereit melden.

`NoteCaughtUp` sammelt die Partitionen, die einmal bis ans Ende gelesen wurden. Erst wenn
*jede* zugewiesene Partition dabei ist, fällt die Bereitschaftsschranke
(`BrokerReadiness.MarkHistoryLoaded`). Die Zuweisung wird dafür vom Consumer selbst gelesen,
nicht aus dem Assignment-Handler gemerkt: das ist die Menge, die librdkafka tatsächlich
liefert, und eine leere Menge heißt, dass noch nichts zugewiesen ist.

Ein Pod, der erst das halbe Topic gelesen hat, würde einem Browser ein halbes Gespräch zeigen.
Deshalb bleibt er bis dahin aus den Service-Endpoints.

Nachgeladene Nachrichten zählt `redepanda_messages_received_total` **nicht** mit. Sie wurden
gezählt, als sie zum ersten Mal ankamen. Zählte man sie erneut, spränge der Zähler bei jedem
Pod-Neustart um die gesamte Retention — er soll den laufenden Chat abbilden, nicht das eigene
Nachladen.

## Producer

`EnableIdempotence = true` ist das, was einen Retry *sicher und in der richtigen Reihenfolge*
macht. Ohne Idempotenz darf librdkafka einen wiederholten Record nach einem später erzeugten
ausliefern. Das ist hier nicht bloß eine Dublette, die man tolerieren könnte: der Resume-Filter
im Frontend verwirft alles, dessen Offset nicht größer ist als der zuletzt gesehene. Ein
umsortierter Retry wäre eine still verlorene Nachricht.

Idempotenz impliziert `acks=all`, begrenzt die offenen Requests und schaltet Retries ein.
`Acks = Acks.All` steht trotzdem ausdrücklich da: librdkafka lehnt die Kombination sonst ab,
und ein ausgeschriebener Wert liest sich besser als ein implizierter.

`PRODUCE_TIMEOUT_MS` (Vorgabe 10 000) setzt librdkafkas `message.timeout.ms`. Dessen Vorgabe
ist **300 000** — fünf Minuten. Die ist für eine Hintergrund-Pipeline gedacht. Dieser Producer
sitzt im Anfrageweg eines HTTP-POST: Bei einem nicht erreichbaren Broker hielte der Browser
seine Anfrage fünf Minuten offen, mit blockiertem Eingabefeld, um am Ende zu erfahren, was
schon die ersten Sekunden entschieden hatten.

`RequestTimeoutMs` ist die Hälfte davon. Läge es auf oder über dem Message-Timeout, verbrauchte
ein einziger offener Request das ganze Budget, und der Retry, den das Message-Timeout erlaubt,
fände nie statt.

Der Schlüssel jedes Records ist der Raumname. Damit bleibt die Reihenfolge innerhalb eines
Raums auch dann erhalten, wenn das Topic später mehr Partitionen bekommt.

Fehlerabbildung nach außen (`StatusCodeFor`): Ein Timeout heißt, dass die Anfrage überhaupt
nicht beantwortet wurde — das ist **504**. Alles, was der Broker aktiv abgelehnt hat, ist
**502**.

## Fehler und Protokollierung

Drei Kafka-Clients laufen im Backend: Producer, Consumer und der Admin-Client der
Bereitschaftsprüfung. Alle drei setzen einen `LogHandler`. Ohne den schreibt librdkafka seine
Diagnose direkt nach stderr — außerhalb von `LOG_LEVEL` und außerhalb des JSON, das die
Plattform einsammelt.

Beim Admin-Client fiel das besonders unangenehm auf: librdkafka nennt ihn intern
`rdkafka#producer-N`. Seine rohen `%3|...|FAIL|`-Zeilen landeten also auf stderr und sahen
dabei auch noch nach einem Producer aus.

`LogThrottle` lässt eine Meldung je Intervall (10 s) durch. Grund: librdkafka ruft den
Error-Callback je Verbindungsversuch auf, ein einziger nicht erreichbarer Broker erzeugte damit
rund zwanzig Zeilen pro Sekunde und Pod. Das Log wurde für alles andere unbrauchbar, genau in
dem Moment, in dem jemand hineinsieht.

Die zurückgehaltene Anzahl reist auf der nächsten durchgelassenen Zeile mit. Ein gedrosseltes
Log bleibt so ehrlich: es faltet Wiederholung, es versteckt sie nicht.

**Metriken werden bewusst nicht mitgedrosselt.** Ein Zähler, der Ereignisse auslässt, ist kein
Zähler mehr. `redepanda_kafka_errors_total` soll die echte Rate zeigen.

Zustellfehler erreichen den Error-Callback nicht — librdkafkas `error_cb` meldet nur Ereignisse
auf Client-Ebene. Deshalb zählt `ProduceAsync` sie selbst, im `catch`.

Die Umsetzung der Syslog-Level: librdkafkas Verbindungsgeplauder liegt auf `notice` und `info`,
erst ab `warning` beschreibt es etwas, worauf jemand reagieren kann.

## Ein Pod, der nicht konsumieren kann, ist kaputt — nicht eingeschränkt

Der Aufbau des Consumers steht **innerhalb** des `try`, nicht davor. Das ist nicht kosmetisch.

Den Consumer zu bauen liest die Broker-Sicherheitseinstellungen, und eine fehlende wirft.
`Subscribe` gegen einen Broker, der die Group ablehnt, wirft ebenfalls. Stand das außerhalb des
`try`, erreichte der Fehler `BackgroundService`. Dessen Vorgabeverhalten `StopHost` bittet den
Host, *geordnet* zu beenden. Die Folge: Der Prozess endete mit Exit-Code 0, Kubernetes meldete
„Completed" statt eines Absturzes, es folgte kein Neustart — und `/health/live` antwortete
weiter aus einem Pod, der nie eine Nachricht ausliefern würde.

Jetzt endet ein solcher Fehler mit `LogCritical`, `Environment.ExitCode = 1` und
`StopApplication()`. Der Neustart sieht dann auch wie ein Fehlschlag aus.

Ein einzelner unlesbarer Record beendet die Schleife dagegen nicht — er wird gezählt, geloggt
und übersprungen.

Die Consume-Schleife läuft über `Task.Factory.StartNew` mit `TaskCreationOptions.LongRunning`
auf einem eigenen Thread. `Consume(CancellationToken)` blockiert; auf einem Thread-Pool-Thread
startete der Host keinen weiteren Dienst, bis diese Methode zurückkehrt.

## Herunterfahren

`Close()` sagt dem Group-Coordinator Bescheid, statt ihn auf den Session-Timeout warten zu
lassen. Der Aufruf ist ein blockierender Roundtrip und **nimmt kein CancellationToken**.
`HostOptions.ShutdownTimeout` begrenzt ihn deshalb nicht: dieses Timeout bricht das Token ab,
das an `StopAsync` übergeben wird, und dieser Aufruf hat keins. Gegen einen unerreichbaren
Broker lief die Wartezeit über das gesamte Shutdown-Budget und endete in einem SIGKILL, der
nichts protokolliert.

`TryCloseWithin` legt `Close()` und `Dispose()` auf einen eigenen Thread und wartet höchstens
`CloseBudget` (5 s) darauf. Der Thread ist ein Hintergrund-Thread, eben damit man ihn aufgeben
kann: Einer, der nach Ablauf des Budgets noch hängt, hält den Prozess nicht offen — und zu dem
Zeitpunkt geht der Pod ohnehin. Ein Thread-Pool-Element wäre falsch, es blockierte einen
Pool-Thread so lange, wie der Broker wegbleibt.

`Dispose()` steht mit im Budget: Ein `Dispose` nach einem aufgegebenen `Close` blockierte am
selben unerreichbaren Broker.

Das Gesamtbudget beim Rollout:

| Schritt | Dauer |
|---|---|
| `preStop`-Hook (`sleep 5`) | 5 s |
| Hosted Services stoppen (`HostOptions.ShutdownTimeout`) | ≤ 25 s |
| davon Consumer-`Close` (`CloseBudget`) | ≤ 5 s |
| Producer-`Flush` bei der Container-Entsorgung | ≤ 5 s |
| **Summe im schlechtesten Fall** | **≈ 35 s** |
| `terminationGracePeriodSeconds` | 45 s |

Der Flush ist nötig, damit eine mit `202` angenommene Nachricht bei SIGTERM nicht verloren
geht. Er liegt hinter dem Shutdown-Timeout, weil die Container-Entsorgung nach `StopAsync`
läuft — daher die getrennte Zeile in der Tabelle.

## Bereitschaftsprüfung gegen den Broker

`BrokerReadiness` fragt Metadaten ab und cached die Antwort 5 s. Die Probe läuft alle paar
Sekunden; ein Metadaten-Roundtrip je Probe wäre sinnlose Last auf dem Broker.

Die `_historyLoaded`-Schranke davor wird **nicht** gecached: Das ist ein einzelner
`volatile`-Lesezugriff, und er muss bei der nächsten Probe wirken.

Ein Broker-Fehler wird auf **Warning** geloggt, nicht auf Debug. Das ist die einzige Auskunft,
die ein Pod darüber gibt, warum er seine Bereitschaftsprüfung nicht besteht. Das Chart läuft
auf `Information` — auf Debug war die Meldung also genau dann unsichtbar, wenn man sie
brauchte. Zum Log-Sturm kann sie nicht werden, weil der Cache sie auf eine Zeile je 5 s
begrenzt.

## Abgesicherte Broker

`RedePanda.Contracts.KafkaSecurity` ist die einzige Stelle, an der TLS und SASL auf eine
`ClientConfig` abgebildet werden. Sie liest fünf Umgebungsvariablen:

| Variable | Bedeutung |
|---|---|
| `REDPANDA_SECURITY_PROTOCOL` | `Plaintext`, `Ssl`, `SaslPlaintext`, `SaslSsl` |
| `REDPANDA_SASL_MECHANISM` | z. B. `SCRAM-SHA-512` |
| `REDPANDA_SASL_USERNAME` | |
| `REDPANDA_SASL_PASSWORD` | |
| `REDPANDA_SSL_CA_LOCATION` | CA-Bündel für eine private CA |

Das ist der Preis dafür, dass „der Backing Service ist über Konfiguration austauschbar" auch
stimmt. Der Broker im Chart spricht Plaintext auf einem clusterinternen Service. Jeder Broker
außerhalb davon — managed Redpanda, ein geteiltes Kafka, irgendetwas über ein fremdes Netz —
braucht TLS, Zugangsdaten oder beides. Ohne diese Klasse wäre ein Umbiegen von
`REDPANDA_BOOTSTRAP_SERVERS` eine Codeänderung, also genau das, was die Behauptung ausschließt.

Die Klasse liegt in `Contracts`, das sonst keine Kafka-Abhängigkeit hätte. Grund:
`ClientConfig` ist die gemeinsame Basis von Producer-, Consumer- und Admin-Konfiguration. Eine
Abbildung bedient alle sieben Client-Stellen im Repository; sieben Kopien davon liefen
auseinander. `Contracts` besitzt ohnehin schon das andere, worüber sich alle Clients einig sein
müssen: das Wire-Format.

Diese Zahl ist es wert, ehrlich gehalten zu werden. Sie stand auf „fünf", während es sieben
waren. Die zwei nicht mitgezählten waren die beiden Admin-Clients — und einer davon war ohne
`ApplyTo` ausgeliefert worden und ließ gegen einen abgesicherten Broker jede
Bereitschaftsprüfung scheitern.

### Wo die Klasse absichtlich wirft

- **CA gesetzt, aber Protokoll ohne TLS.** Nicht bloß nutzlos: Ein CA-Bündel hier heißt, dass
  jemand diese Verbindung für verschlüsselt hält. Sie ist es nicht, und darüber zu schweigen
  wäre die schlechteste aller Möglichkeiten.
- **SASL-Protokoll ohne Mechanismus, Benutzer oder Passwort.** Ein Client, der ohne
  Zugangsdaten startet, scheitert an jeder Verbindung und meldet das als Broker-Problem. Der
  Weg von dort zur eigentlichen Ursache ist deutlich länger als eine Meldung beim Start.

Nicht gesetzte Werte lassen die Config unangetastet — Plaintext ist librdkafkas eigene Vorgabe.
Die mitgelieferte Plaintext-Demo verhält sich damit exakt so wie vor Einführung der Klasse.

`SslCaLocation` bleibt ungesetzt, wenn keine CA angegeben ist. Dann nutzt librdkafka den
System-Truststore, was für einen Broker mit öffentlich vertrautem Zertifikat richtig und nur
für eine private CA falsch ist.

Beim Parsen werden `_` und `-` entfernt. Damit wird die Schreibweise akzeptiert, die in der
Dokumentation jedes Brokers steht (`SASL_SSL`, `SCRAM-SHA-512`), und ebenso die des Enums.
Genau das abzulehnen, was die Doku vorgibt, wäre ein Rätsel und kein Schutz.
