# Frontend

Das Frontend ist ein Caddy-Pod, der vier statische Dateien ausliefert und alles unter `/api`
an das Backend weiterreicht. Kein npm, kein Bundler, kein CDN, keine Webfonts.

Das ist kein Sparprogramm, sondern die Grundlage einer Zusage: Im Netzwerk-Tab des Browsers
tauchen nur diese Dateien und `/api/...` auf. Damit ist nachweisbar, dass der Browser kein Kafka
spricht — und es funktioniert in einem Cluster ohne Internetzugang.

## HTTP/1.1 zum Backend, und warum das die zweite Replica erst wirksam macht

Das ist die unauffälligste und folgenreichste Zeile im Caddyfile:

```caddy
transport http {
    versions 1.1
}
```

Überließe man das der ALPN-Aushandlung, käme h2 heraus. Gos HTTP/2-Transport multiplext dann
**jeden Stream über eine einzige TCP-Verbindung** zur selben Upstream-Adresse.

`BACKEND_HOST` löst zu genau einer ClusterIP auf, und kube-proxy wählt den Backend-Pod **je
Verbindung**. Mit h2 liefe damit jeder SSE-Stream dieses Caddy-Pods über dieselbe Verbindung
und landete auf demselben Backend-Pod — gleichgültig, wie viele Replicas laufen.

Über HTTP/1.1 ist jeder SSE-Stream eine eigene Verbindung und wird einzeln verteilt. Der Preis
ist eine Verbindung je offenem Stream. Für einen Chat ist das der richtige Tausch, und die
Zwei-Replica-Demo aus README Abschnitt 8 hängt daran.

## Caddy im Übrigen

**`auto_https off`.** Das Zertifikat stellt das Helm-Chart aus und mountet es. `auto_https`
ließe den Pod stattdessen versuchen, aus dem Cluster heraus Let's Encrypt zu erreichen. `off`
statt `disable_redirects`, weil die Weiterleitung von `:8080` weiter unten ausdrücklich
geschrieben steht — Caddys eigene Variante gibt es nur für Sites mit Hostnamen, und diese haben
keinen.

**`admin off` und `persist_config off`.** Die Admin-API öffnete `:2019` und wollte zusammen mit
`persist_config` nach `/config` schreiben. Beides aus, damit der Container mit
schreibgeschütztem Root-Dateisystem läuft.

**`tls`, nicht `tls_insecure_skip_verify`.** Das Backend-Zertifikat ist von derselben
Release-CA signiert, die dieser Pod trägt, und CN wie erster SAN sind der Service-Name — also
genau das, wozu `BACKEND_HOST` auflöst. Die Namensprüfung geht ohne Override durch.
`tls_trust_pool` statt `tls_trusted_ca_certs`: Letzteres ist seit Caddy 2.11 veraltet und warnt
bei jedem Laden der Konfiguration.

**Das `/api`-Präfix wird durchgereicht, nicht abgeschnitten.** Das Backend antwortet damit auf
denselben Pfaden, ob es über diesen Proxy oder zum Debuggen per Port-Forward erreicht wird.
Caddy streamt `text/event-stream` von sich aus ungepuffert; SSE braucht hier keine weiteren
Direktiven.

### `grace_period 5s`

Ohne das lässt SIGTERM Caddy unbegrenzt auf das Ende laufender Anfragen warten. Eine
SSE-Antwort endet nie — dafür ist sie da. Der Pod saß deshalb bei **jedem** rollierenden Update
bis zum SIGKILL des kubelet bei `terminationGracePeriodSeconds`.

Die README behauptete, das Frontend fahre analog zum Backend herunter. Das stimmte schlicht
nicht: Das Backend schneidet seine Streams bei `ApplicationStopping`, diese Seite tat gar
nichts.

Die 5 s passen zum `preStop`-Sleep im Chart. Wenn sie ablaufen, ist der Pod bereits aus den
Service-Endpoints heraus. Die abgeschnittenen Streams gehören also zu Browsern, die woanders
hin können — und sie verbinden sich mit `Last-Event-ID` neu und verlieren nichts.

### Logging hat zwei getrennte Schalter

Das ist eine Falle, die still zuschnappt.

`log` im **globalen** Block benennt einen Logger für Caddys eigene Prozessereignisse: Start,
Konfiguration geladen, TLS-Storage-Durchlauf, Wählfehler zum Upstream.

**Zugriffsprotokolle entstehen davon nicht.** Die gibt es nur, wenn `log` *innerhalb* eines
Site-Blocks steht. Das Fehlen ist geräuschlos — `caddy validate` läuft in beiden Fällen durch.

Deshalb steht `log` dreimal in der Datei: global, in der TLS-Site und in der
Redirect-Site. Ein Site-Logger schreibt außerdem per Vorgabe nach stderr, `output stdout` muss
also jedes Mal ausgeschrieben werden.

Der Redirect-Site einen eigenen Logger zu geben ist kein Zierrat: „Der Browser ging auf http://
und es passierte nichts" ist genau die Situation, wegen der jemand diese Logs liest.

`/healthz` trägt `log_skip`. Liveness und Readiness fragen alle 10 s je einmal an; ihre
Einträge überträfen den echten Verkehr um ein Mehrfaches. Die Proben laufen weiter, sie
erzählen nur nicht davon.

### Port 8080

Dort wird nichts ausgeliefert. Der Port bleibt offen, damit eine `http://`-Adresse, die jemand
noch hat — ein Lesezeichen, eine ältere README, die URL, die `scripts/demo.sh` einmal ausgab —
mit einer Weiterleitung antwortet statt mit einem Verbindungsabbruch, der wie eine Störung
aussieht.

**308, nicht 301.** Das erhält Methode und Body: Ein `POST /api/messages`, das hier ankommt,
wird über TLS auf denselben Pfad wiederholt, statt still zu einem GET zu werden.

Der Port in der Weiterleitung ist der, den der **Client** erreichen kann. Unter
`kubectl port-forward` ist das der weitergeleitete Port und nicht der dieses Containers. Daher
eine Umgebungsvariable mit dem Demo-Port als Vorgabe statt `{server_port}`.

## `app.js`

528 Zeilen ohne Framework. Die nicht offensichtlichen Teile:

### Zwei Reconnect-Pfade, nicht einer

`EventSource` verbindet sich bei einem *nicht fatalen* Abbruch selbst neu und schickt dabei
`Last-Event-ID` mit. Der Server spielt dann nur nach, was danach kam.

Ist der Fehler **fatal** — der Pod ist weg, Caddy antwortet 502 —, gibt `EventSource`
endgültig auf. Laut Spezifikation gibt es danach keinen weiteren Versuch. Von da an muss das
Frontend selbst neu verbinden, und es baut dafür ein **neues** `EventSource`.

An ein neues `EventSource` kann kein JavaScript einen Header hängen. Der Server sieht also
einen Erstbesucher und spielt den ganzen Raum noch einmal ein.

Unterschieden werden die beiden am `readyState` im Fehler-Handler: `CONNECTING` heißt, die
Bibliothek versucht es weiter; `CLOSED` heißt, der Versuch erreichte einen Server und bekam
eine Antwort, die kein SSE-Stream war.

### Der Offset-Filter

Deshalb merkt sich der Client den höchsten bereits gerenderten Offset und verwirft alles, was
nicht darüber liegt. Das ist es, was den zweiten Reconnect-Pfad davor bewahrt, als zweite Kopie
des Gesprächs anzukommen — und es gilt für beide Pfade, auf welchem Pod der Reconnect auch
landet.

Der Zähler wird **vor** dem Rendern hochgesetzt und unabhängig davon, ob das Rendern gelingt:
Der Frame ist so oder so verbraucht, und einen, den man nicht parsen konnte, will man nicht
erneut versuchen. Ein Frame ohne `id` ergibt `NaN` und gilt als neu — die sichere Richtung.
Heartbeats kommen ohnehin nie hier an, weil sie den Event-Typ `ping` tragen.

Beim Raumwechsel wird der Zähler zurückgesetzt. Offsets gehören dem Topic, nicht einem Raum:
Die Nachrichten des nächsten Raums können niedrigere tragen als die zuletzt gerenderten und
würden sonst als „schon gesehen" weggefiltert.

### Backoff

Acht Versuche mit 1, 2, 4, 8 s und dann am Deckel — zusammen rund 75 s. Lang genug, dass ein
neu eingeplanter Backend-Pod bereit wird; kurz genug, dass ein wirklich totes Backend nicht
ewig weiterversucht.

Dazu Jitter, damit ein Raum voller Tabs den neuen Pod nicht in einem synchronisierten Schwall
trifft.

Ein Stream, der tatsächlich aufging, bekommt ein frisches Budget. Zurück ins Netz zu kommen
(`online`) ist neue Information und belebt ein bereits verbrauchtes Budget wieder; ein Tab, den
man nur wieder ansieht, ist das nicht und verkürzt lediglich eine ohnehin laufende Wartezeit.

Der Sendeversuch hat ein eigenes Timeout, bewusst **über** `PRODUCE_TIMEOUT_MS` (10 s) des
Backends. Der Normalfall soll sein, dass der Server mit 504 antwortet und sagt, warum. Das
Frontend-Timeout ist der Rückfall für den Fall, dass der Server gar nicht antworten kann.

### Kleinigkeiten mit Grund

- **`textContent`, nie `innerHTML`** — die Zeichenkette kommt von einem anderen Nutzer.
- **`Array.from`, nicht `[0]`**, für den Anfangsbuchstaben des Nicknamens: Ein Name kann mit
  einem Emoji beginnen, und Indizierung nähme die Hälfte eines Surrogatpaars.
- **Vor dem DOM-Zugriff entscheiden**, ob der Leser unten steht — sonst verändert die gerade
  hinzugefügte Höhe die Antwort.
- **Sprunghaft scrollen, nicht animiert.** Eine laufende Scroll-Animation ist noch nicht fertig,
  wenn die nächste Nachricht ankommt; `isNearBottom()` mäße dann die Animation statt der
  Leserposition und schlösse fälschlich, der Leser sei weggescrollt. Nur der Sprung-Button
  animiert.
- **Zeitstempel kommen als UTC** und werden in der lokalen Zeit des Betrachters gerendert. Die
  Liste zeigt HH:MM, der genaue Moment bleibt im Tooltip.
- **Ohne Stream schließt das Eingabefeld.** Es gäbe nichts, worauf eine gesendete Nachricht
  erscheinen könnte, und ein POST schlüge ohnehin sehr wahrscheinlich fehl — Eingabe still zu
  verlieren wäre schlechter.

## Theme

Ein Betrachter ist in einem von **drei** Zuständen: keine ausdrückliche Wahl (dann gilt nur
`prefers-color-scheme`), `data-theme="light"` oder `data-theme="dark"`.

Daraus folgt der Aufbau von `style.css`: Die helle Palette steht auf blankem `:root`. Die
dunkle steht **zweimal** — einmal hinter der Media-Query, abgesichert gegen eine ausdrückliche
Hellwahl, und einmal hinter dem Attribut, damit der Schalter gegen das Betriebssystem gewinnt.
Jede Komponente liest nur Tokens.

Eine Farbe, die ausschließlich in einem Media- oder Attributblock deklariert wäre, würde im
ungestempelten Zustand nicht greifen — und der ist der Mehrheitsfall.

Die Neutraltöne sind zum Akzent hin verschoben statt reine Graustufen zu sein: Es ist ein roter
Panda, also warmes Umbra als Grund und das Rostrot des Fells als Akzent.

Der Farbton je Nickname wird gehasht; Helligkeit und Sättigung kommen aus dem Theme, damit
jeder erzeugte Ton auf beiden Hintergründen lesbar bleibt. Eigene Nachrichten erkennt man an
Position und Label „(du)", nicht an der Farbe allein.

Der Schalter merkt sich die Wahl. Ist der Speicher nicht verfügbar — privates Fenster —, gilt
weiter die Systemeinstellung; die Wahl wirkt dann für diese Seite, überlebt aber kein Neuladen.

## Das Image

Zwei Stufen. Die erste ist das Caddy-Image, die zweite blankes Alpine.

**Warum nicht einfach das Caddy-Image ausliefern:** Alpine ist etwa halb so groß und bringt die
Volumes `/config` und `/data` sowie `EXPOSE 80/443/2019` nicht mit, die hier weder gebraucht
noch gewollt sind.

**Alles wird kopiert, nichts installiert.** Ein `apk add` griffe zur Bauzeit auf einen
Alpine-Mirror zu und löste auf, was dieser an dem Tag ausliefert — das wäre die eine ungepinnte
Zutat in einem sonst digest-gepinnten Image. Die Caddy-Stufe installiert genau diese Dateien
bereits upstream und ist per Digest gepinnt.

Kopiert werden:

- `mime.types` — ohne die rät der File-Server die Content-Types, und `.css` und `.js` kämen als
  `application/octet-stream` an, die der Browser verweigert.
- `ca-certificates.crt` — der Proxy-Upstream wird gegen die Release-CA geprüft, hier signiert
  also heute nichts etwas. Go baut seinen Root-Pool aber aus diesen Dateien, und ein leerer Pool
  machte aus jedem künftigen öffentlichen Endpunkt ein wenig hilfreiches
  „x509: certificate signed by unknown authority".

**`caddy adapt`, nicht `caddy validate`.** Das ist kein Rückschritt: `validate` *provisioniert*
die Konfiguration, lädt also die TLS-App und öffnet die Zertifikatsdateien. Die werden erst beim
Deployment aus einem Secret gemountet und existieren zur Bauzeit nicht — `validate` scheitert
deshalb an einem völlig intakten Caddyfile mit
„open /etc/redetim/tls/tls.crt: no such file or directory". `adapt` benutzt denselben Parser
und weist dieselben Syntax- und Direktivenfehler zurück, ohne das Dateisystem anzufassen. Genau
das ist die Fehlerklasse, die ein Build noch abfangen kann.

**`setcap -r /usr/bin/caddy`.** Das veröffentlichte Binary trägt `CAP_NET_BIND_SERVICE`, um
`:80` und `:443` binden zu können. Hier wird auf `:8443` und `:8080` gehört, beide über 1024,
und im Pod werden alle Capabilities fallen gelassen. Die Dateicapability wird entfernt, damit
beides zueinander passt.

**`XDG_DATA_HOME` und `XDG_CONFIG_HOME`.** Auch mit `auto_https off` instanziiert Caddy beim
Start seine TLS-App und durchläuft deren Storage-Verzeichnis. Ohne diese Variablen löst das zu
`$HOME/.local/share/caddy` auf, das es auf einem schreibgeschützten Root-Dateisystem nicht gibt
— und Caddy protokolliert bei jedem Start einen Storage-Fehler. Der Pod mountet an beiden
Pfaden ein `emptyDir`.

**Zertifikat und Schlüssel sind nicht eingebacken.** Sie werden aus dem Secret des Charts unter
`/etc/redetim/tls` gemountet. Ein Zertifikat ist Deployment-Konfiguration; ein Image, das eins
trüge, wäre ein Release, das sich nur ein einziges Mal ausrollen ließe (12-Factor III).

Allgemeines zu Digest-Pinning und Reproduzierbarkeit steht in [build.md](build.md).
