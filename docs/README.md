# Entwurfsdokumentation

Diese Dokumente erklären, **warum** RedeTim so gebaut ist, wie es gebaut ist. Sie sind
das Gegenstück zur [README](../README.md): die README sagt, wie man das System baut,
installiert und bedient — hier steht die Begründung dahinter.

Die meisten Abschnitte halten eine Entscheidung fest, die auch anders hätte ausfallen können,
und den Grund, warum sie so ausgefallen ist. Häufig ist dieser Grund ein Fehler, der einmal
aufgetreten ist. Wer eine dieser Stellen ändern will, sollte den zugehörigen Abschnitt vorher
gelesen haben.

## Lesereihenfolge

Für einen ersten Überblick reichen die ersten beiden Dokumente.

| Dokument | Inhalt |
|---|---|
| [architecture.md](architecture.md) | Welche Dienste es gibt, wie sie zusammenhängen, welche Kopplungen niemand automatisch prüft |
| [kafka.md](kafka.md) | Producer, Consumer, Offsets, eine Consumer-Group je Pod, Broker-Absicherung |
| [streaming.md](streaming.md) | Der SSE-Weg vom Topic in den Browser: Verlauf, Backpressure, Wiederaufnahme |
| [frontend.md](frontend.md) | Caddy als Reverse Proxy, `app.js` ohne Build-Tooling, Theme-Modell |
| [observability.md](observability.md) | OTel-SDK, Collector, Prometheus, die Namensregeln für Metriken |
| [deployment.md](deployment.md) | Helm-Chart, TLS, Probes, Shutdown, HPA |
| [build.md](build.md) | Zentrale Build-Konfiguration, Images, Digest-Pins, Reproduzierbarkeit |

## Verhältnis zur README

Die README bleibt maßgeblich für alles Bedienbare: Voraussetzungen, Befehle,
Konfigurationstabelle, 12-Factor-Bewertung, CNCF-Technologien, Abnahmeliste, bekannte
Einschränkungen. Ihre Abschnittsnummern sind stabil und werden aus dem Code heraus
referenziert.

Diese Dokumente wiederholen davon nichts. Wo ein Thema beides hat — Bedienung und Begründung —
steht die Bedienung in der README und die Begründung hier.

## Warum eine eigene Ablage

Die Begründungen standen früher als Kommentare im Code. Das hatte drei Nachteile: sie waren
nur zu finden, wenn man ohnehin schon in der richtigen Datei war; ein zusammenhängender
Überblick über den Entwurf ließ sich nirgends lesen; und einzelne Absätze waren länger als
der Code, den sie erklärten.

Der Code trägt jetzt fast keine Prosa mehr. Was dort noch steht, ist entweder funktional
(die `--help`-Köpfe der Skripte) oder hält eine Kopplung fest, die sonst niemand bemerkt —
siehe [architecture.md](architecture.md#manuelle-kopplungen).
