# Observability

Der Weg ist: **Backend → OTLP → Collector → Prometheus**. Das Backend pusht, es wird nie
gescrapt. Es hat gar keinen `/metrics`-Endpunkt — das ist der Sinn davon, einen Collector
davorzustellen, und zugleich der Beweis, dass der Weg wirklich so verläuft.

Die Bedienung — PromQL-Beispiele, Targets prüfen, Screenshots — steht in README Abschnitt 10.
Hier steht, warum die Kette so aufgebaut ist.

## Die Instrumente

Fünf, nicht vier. Vier Counter in `ChatMetrics` und ein observabler UpDownCounter, den
`ChatBroadcaster` anlegt. Die Tabelle mit den Prometheus-Namen steht in README Abschnitt 10 —
dort braucht man sie, beim Schreiben von PromQL.

`redetim_streams_cut_total` ist einen Alarm wert, sobald es mehr als selten vorkommt: Es
heißt, dass Leser dem Raum nicht folgen können. Wie es zustande kommt, steht in
[streaming.md](streaming.md#backpressure-was-passiert-wenn-ein-browser-nicht-mitkommt).

Warum `redetim_messages_received_total` beim Nachladen nicht mitzählt, steht in
[kafka.md](kafka.md#wann-ein-pod-bereit-ist).

### Namensregeln

Die Übersetzung nach Prometheus passiert im Prometheus-Exporter des Collectors, nicht im
Backend. Daraus folgen drei Regeln für die Instrumentnamen im Code:

- Punktgetrennte Kleinschreibung.
- **Kein** `_total`-Suffix — der Exporter hängt es an monotone Counter selbst an.
- **Keine** Einheit — eine Einheit `"1"` auf einer Gauge erzeugte ein `_ratio`-Suffix.

Im Collector steht `translation_strategy: UnderscoreEscapingWithSuffixes` ausdrücklich da,
statt sich auf das Vorgabeverhalten zu verlassen. Das ist die Zeile, die aus
`redetim.messages.sent` am Ende `redetim_messages_sent_total` macht.

Zusätzlich zu den eigenen Metriken liefert die ASP.NET-Core-Instrumentierung die
HTTP-Serverhistogramme. Der Kestrel-Meter wird **einzeln** hinzugefügt:
`AddAspNetCoreInstrumentation()` schaltet nur den Meter
`Microsoft.AspNetCore.Hosting` frei.

## Die Resource wird absichtlich nicht im Code gesetzt

Im Backend steht kein `ConfigureResource(r => r.AddService(...))`. Das ist der Kern der
Metrik-Identität und leicht kaputtzumachen.

`OTEL_SERVICE_NAME` und `OTEL_RESOURCE_ATTRIBUTES` sind spezifizierte Umgebungsvariablen, die
das SDK selbst liest. Ein `AddService(...)`-Aufruf im Code schlüge sie: In `Resource.Merge`
gewinnt der Code gegen die Umgebung. Zusätzlich erzeugte `AddService` eine zufällige
`service.instance.id`. Das Prometheus-Label `instance` wäre dann nach jedem Neustart eine
frische GUID — und damit jede Zeitreihe je Pod nach jedem Rollout eine andere.

Stattdessen setzt das Chart:

```yaml
- name: OTEL_RESOURCE_ATTRIBUTES
  value: "service.instance.id=$(POD_NAME)"
```

Das expandiert der kubelet aus `POD_NAME`, das seinerseits per `fieldRef` auf `metadata.name`
zeigt. So wird das `instance`-Label der Pod-Name, ohne dass die Anwendung irgendetwas
konfiguriert.

Gesetzt wird das, sobald Telemetrie überhaupt an ist — nicht nur beim mitgelieferten
Collector. Export an einen fremden Collector ist auch Export, und ohne die Variable erfände
das SDK dort wieder eine bei jedem Neustart wechselnde Instance-ID.

**`service.namespace` wird bewusst nicht gesetzt.** Der Prometheus-Exporter des Collectors baut
das `job`-Label als `<service.namespace>/<service.name>`. Gesetzt würde daraus überall
`redetim/redetim-backend`, und die PromQL-Beispiele der README liefen ins Leere.

Auf der Prometheus-Seite gehört `honor_labels: true` dazu. Der Collector setzt `job` und
`instance` bereits aus `service.name` und `service.instance.id`. Ohne `honor_labels`
überschreibt Prometheus beide und schiebt die Originale nach `exported_job` /
`exported_instance` — dieselben Abfragen kämen leer zurück.

## Warum das Zertifikat über den OS-Truststore läuft

`OTEL_EXPORTER_OTLP_CERTIFICATE` fehlt absichtlich, obwohl die Variable spezifiziert ist und
das SDK sie liest.

OpenTelemetry .NET lädt die genannte Datei mit `X509Certificate2.CreateFromPemFile(path)`.
Deren einargumentige Form nimmt laut Dokumentation den **privaten Schlüssel aus derselben
Datei**. Eine CA-Datei, die einen privaten Schlüssel enthält, ist keine CA-Datei, die man
ausliefern sollte. Es gibt also keinen Inhalt, der zugleich das SDK zufriedenstellt und sicher
zu mounten ist; bei allem anderen stürzt das Backend beim Start ab
(„The key contents do not contain a PEM"). Das steht so auf `main`, ist also nicht spezifisch
für die hier gepinnte Version 1.17 — keine Paketversion behebt es.

Die Variable muss außerdem *abwesend* sein und nicht bloß ungenutzt: Der Topic-Job liest diese
ConfigMap vollständig und mountet kein TLS-Secret. Ein Pfad darin benennte eine Datei, die
dieser Pod nicht hat.

Der Ausweg ist ein Init-Container `ca-bundle`. Er hängt die Release-CA an das
System-CA-Bündel an und legt das Ergebnis unter `SSL_CERT_FILE` ab. .NET validiert unter Linux
über OpenSSL, und OpenSSL liest `SSL_CERT_FILE` — das erreicht den OTLP-Exporter also ohnehin,
und zwar zusammen mit jedem anderen TLS-Client im Prozess.

Er **konkateniert**, er ersetzt nicht: `SSL_CERT_FILE` überschreibt den gesamten Truststore.
Eine Datei mit nur der Release-CA ließe das Backend jeder öffentlichen CA misstrauen.

Als Image dient das Backend-Image selbst — ein Image je Release, und kein zweites Basis-Image,
das man für ein `cat` gepinnt und gepatcht halten müsste.

## Collector-Konfiguration

Was daran nicht offensichtlich ist:

- **`memory_limiter` muss der erste Prozessor sein**, sonst erreicht der Gegendruck den
  Receiver nicht.
- **`metric_expiration: 5m` muss über `OTEL_METRIC_EXPORT_INTERVAL` liegen.** Sonst flackern
  Zeitreihen zwischen zwei Scrapes.
- **Der Receiver hört auf `0.0.0.0`, nicht auf `localhost`** — sonst käme aus einem anderen Pod
  nie etwas an.
- **Der `health_check` muss zusätzlich unter `service.extensions` stehen.** Sonst wird der
  Block geparst und nie gestartet, Port 13133 ist tot und die Probes scheitern.
- Der Debug-Exporter heißt `debug`; der frühere `logging`-Exporter wurde in v0.111.0 entfernt.

## TLS, und die eine Ausnahme

Jeder Server, den der Collector öffnet, ist TLS: OTLP über gRPC (4317) und HTTP (4318), der
Prometheus-Exporter (8889) und der Health-Check (13133). Prometheus scrapt 8889 mit
`scheme: https` und prüft gegen die Release-CA, statt `insecure_skip_verify` zu setzen. Ein
`server_name`-Override ist nicht nötig, weil das Target über den Service-Namen adressiert wird
— und der ist CN und erster SAN des Zertifikats.

Die Ausnahme ist **`service.telemetry.metrics` auf Port 8888**, die Selbsttelemetrie des
Collectors. Das ist ein Reader aus dem `opentelemetry-configuration`-Schema, kein
`confighttp`-Server. Dieses Schema gibt einem Pull-/Prometheus-Reader Host, Port und ein paar
Formatierungsschalter — ein Feld für ein Zertifikat gibt es nicht.

Sie bleibt deshalb einfaches HTTP und ist der einzige unverschlüsselte Hop im Release. Sie
trägt keine Chat-Daten, sondern nur die Zähler des Collectors selbst. Weggelassen wurde sie
nicht: Den Blick darauf zu verlieren, ob der Collector still Metriken verwirft, wäre der
schlechtere Tausch. Die Einschränkung ist in README Abschnitt 14 festgehalten.

## Warum ein Collector und nicht `prometheus-net`

Das Backend kennt kein Monitoring-Backend, sondern nur einen OTLP-Endpunkt aus einer
Umgebungsvariable. Das ist dieselbe 12-Factor-Argumentation wie bei Redpanda: eine angehängte
Ressource, austauschbar ohne Rebuild.

Der Preis ist ein Pod und ein Netzwerk-Hop mehr.

Das Chart macht die Aussage auch für sich selbst wahr. `otelCollector.external.endpoint`
erlaubt es, an einen bereits vorhandenen Collector zu exportieren. Bis es das gab, schaltete
das Abschalten des mitgelieferten Collectors das SDK komplett ab — der einzige Weg zu einem
fremden Collector führte über das Bearbeiten des Templates.

Ist weder der mitgelieferte Collector aktiv noch ein externer benannt, wird
`OTEL_SDK_DISABLED: "true"` gesetzt. Das SDK wird also abgeschaltet, statt einen Endpunkt
anzufragen, den es nicht gibt. Berücksichtigt wird das ab OpenTelemetry .NET 1.15.0; das Chart
liefert 1.17.

## Bewusst nur Metriken

Der Collector könnte auch Traces und Logs. Aktiviert ist nur die Metrics-Pipeline.

- **Traces** bräuchten Kontextpropagierung von Hand über die Kafka-Grenze — `Confluent.Kafka`
  hat keine stabile Auto-Instrumentierung — plus ein zweites Backend.
- **Logs** gehen nach stdout, wie es 12-Factor vorsieht. Die Plattform sammelt sie ein.

Der Ausbaupfad bleibt offen: Eine Trace-Pipeline wäre im vorhandenen Collector ein Receiver-
und ein Exporter-Block.

## Logging

Das Backend schreibt strukturiertes JSON nach stdout, keine Logdateien.

Der JSON-Formatter bekommt einen Zeitstempel gesetzt. Ohne den trägt das JSON **überhaupt
keine** Ereigniszeit — ein Ereignisstrom, dessen Ereignisse sich weder ordnen noch mit etwas
anderem korrelieren lassen, ist deutlich weniger ein Log, als er aussieht.

Das Format ist rundreisefähig und ausdrücklich UTC, damit niemand stromabwärts einen Offset
raten muss. Ohne abschließendes Leerzeichen: Das ist eine Konvention des
Klartext-Konsolenformatters, der die Zeitangabe von der Meldung trennen muss, und in JSON
verfälschte es nur den Feldwert.

ASP.NET-Framework-Logs werden ab `Information` auf `Warning` gefiltert. Das Framework schreibt
etwa sechs Zeilen je Anfrage, und die meisten Anfragen hier sind Bereitschaftsproben — die
eigenen Ereignisse der Anwendung waren damit eine Minderheit im eigenen Log. Es ist das
Gegenstück zu `log_skip` auf `/healthz` im Caddyfile.

Unterdrückt wird nur, solange niemand tatsächlich debuggt. Wer `LOG_LEVEL=Debug` setzt, will
alles; die Framework-Hälfte still zurückzuhalten wäre ein eigenes Rätsel.

Wie librdkafkas eigene Ausgabe in denselben Strom kommt und warum sie gedrosselt wird, steht in
[kafka.md](kafka.md#fehler-und-protokollierung).
