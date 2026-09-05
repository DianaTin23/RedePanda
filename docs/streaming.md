# Der Weg vom Topic in den Browser

Der Browser bekommt Nachrichten über **Server-Sent Events**. Das Backend hält je Verbindung
einen Kanal, füttert ihn aus dem Kafka-Consumer und schreibt die Frames auf die offene
HTTP-Antwort.

Wie gelesen und geschrieben wird, steht in [kafka.md](kafka.md). Hier geht es um alles ab dem
Moment, in dem `ChatBroadcaster.Publish` aufgerufen wird.

## Warum SSE und kein WebSocket

Redpanda ist die Backplane. Jeder Pod konsumiert das ganze Topic und kennt damit jede
Nachricht, unabhängig davon, an welchem Pod ein Browser hängt. Das erspart alles, was man
sonst für einen mehrfach betriebenen Chat braucht: kein SignalR-Backplane, keine
Sticky Sessions, keine Weitergabe zwischen Pods.

Übrig bleibt eine Einbahnstraße vom Server zum Browser. Genau dafür ist SSE gedacht. Der
Rückweg — eine Nachricht senden — ist ein gewöhnlicher `POST /api/messages`.

Der entscheidende Zusatznutzen ist die Wiederaufnahme: SSE hat sie eingebaut. Der Browser
schickt beim automatischen Reconnect die zuletzt gesehene Event-ID im Header
`Last-Event-ID` mit. Als ID wird der **Kafka-Offset** verwendet. Damit wird aus dem Nachladen
eine echte Fortsetzung statt einer zweiten Kopie des Raums — und zwar auch dann, wenn der
Browser bei einem anderen Pod landet: der Offset gehört dem Broker, nicht dem Pod. Eindeutig
ist er **je Partition**; dass das hier reicht, liegt am Raum als Record-Key (siehe
[architecture.md](architecture.md#ein-topic-für-alle-räume)).

## Prozesslokaler Zustand

`ChatBroadcaster` hält die SSE-Verbindungen dieses Pods und den Verlauf, den er gesehen hat.
Beides ist bewusst prozesslokal.

Eine SSE-Verbindung gehört genau einem Pod und lässt sich nicht verschieben. Der dauerhafte
Verlauf liegt weiterhin im Topic; was der Broadcaster hält, ist nur eine Projektion davon, die
`ChatConsumerService` bei jedem Start neu aufbaut.

## Das Schloss und warum es eines gibt

`ChatBroadcaster` serialisiert zwei Dinge gegeneinander:

- Aufzeichnen und Verteilen (`Publish`)
- Schnappschuss nehmen und Abonnenten eintragen (`Subscribe`)

Ohne das Schloss gibt es ein Fenster zwischen dem Schnappschuss einer neuen Verbindung und
ihrem Erscheinen in der Abonnentenliste. Eine Nachricht, die in dieses Fenster fällt, wird
entweder doppelt zugestellt oder gar nicht.

Deshalb ist `ChatHistory` auch **nicht** thread-sicher. Der Broadcaster besitzt die einzige
Instanz und schützt jeden Aufruf mit seinem eigenen Schloss. Ein zweites Schloss dort drin
würde eine Sicherheit suggerieren, die einzelne Aufrufe ohnehin nicht bieten können — die
Atomarität liegt eine Ebene höher.

Das Schloss wird über die gesamte Verteilung gehalten. Das ist unbedenklich, weil darin nichts
blockiert: `TryWrite` gibt bei vollem Puffer `false` zurück, statt zu warten.

## Backpressure: was passiert, wenn ein Browser nicht mitkommt

Jeder Abonnent hat einen begrenzten Kanal, 256 Nachrichten (`SubscriberBufferSize`). Groß
genug, dass ein normal langsames Rendern ihn nie erreicht; klein genug, dass ein Browser, der
wirklich aufgehört hat zu lesen, nicht viel Speicher blockieren kann.

`FullMode` ist `Wait`, und trotzdem wartet niemand jemals darauf: `TryWrite` gibt `false`
zurück, statt zu blockieren, und `Publish` behandelt das als Ende dieses Abonnements.

`DropOldest` wäre die naheliegende Wahl gewesen und war schlechter, als sie aussah. Es hätte
für einen bloß langsamen Leser still eine Nachricht verworfen — ohne Log und ohne dass der
Browser je erfahren hätte, dass er ein Loch hat.

Den Stream zu beenden ist dagegen reparabel:

1. `ChatStream` hält an einem geschlossenen Kanal an.
2. `EventSource` verbindet sich von selbst neu, mit `Last-Event-ID`.
3. Der Replay füllt die Lücke exakt.

Der Abbruch wird geloggt. `TryComplete` steht dabei **innerhalb** der Bedingung: Es liefert
nur beim ersten Mal `true`, sodass ein bereits abgeschnittener und noch nicht entsorgter
Abonnent nicht für den Rest seines Lebens einmal pro Nachricht loggen kann.

## Der Verlaufspuffer

`ChatHistory` ist in **beide** Richtungen begrenzt, in die er wachsen kann:

| Einstellung | Begrenzt | Vorgabe |
|---|---|---|
| `CHAT_HISTORY_SIZE` | Nachrichten je Raum | 200 |
| `CHAT_MAX_ROOMS` | gleichzeitig gehaltene Räume | 200 |

Die zweite Schranke gibt es, weil die erste nicht genügt. `CHAT_HISTORY_SIZE` kürzt die
Warteschlange *innerhalb* eines Raums; die Anzahl der Räume kürzte nichts. Ein Raumname ist
keine feste Menge — er kommt aus einem Query-String oder aus einer Nachricht. Ein unbegrenztes
Wörterbuch davon ist eine Speichergrenze, die derjenige setzt, der mit dem Pod redet. Und zwar
auf **jeder** Replica gleichzeitig, weil jede das ganze Topic konsumiert.

Beide Werte dürfen `0` sein, das heißt weiterhin „unbegrenzt".

### Welcher Raum verdrängt wird

Der Raum, dessen letzte Nachricht am ältesten ist. Also zuletzt *geschrieben*, nicht zuletzt
*gelesen*. Ein Schnappschuss ist ein Lesezugriff; ließe man Lesezugriffe die
Verdrängungsreihenfolge verändern, könnte ein Browser, der einem Raum beitritt, den Verlauf
eines anderen Raums verdrängen — genau die Sorte Überraschung, gegen die diese Schranke
eingeführt wurde.

Entschieden wird über einen Zähler, nicht über eine Uhr. Ein Zeitstempel machte die
Verdrängungsreihenfolge von der Wanduhr des Pods abhängig statt von der Lesereihenfolge des
Topics — und ein Replay liest ein ganzes Topic innerhalb eines Ticks einer groben Uhr.

Was ein Pod hier vergisst, ist das, was er beim Beitritt ausliefern kann. Die Nachrichten
liegen weiter im Topic. Das ist derselbe Tausch, den `CHAT_REPLAY_RECORDS` beim Start schon
macht.

Der Suchlauf ist linear und soll es sein: Er läuft nur, wenn ein *neuer* Raum auftaucht,
während der Puffer voll ist, und über höchstens `CHAT_MAX_ROOMS` Einträge. Eine Struktur, die
das auf O(1) brächte, müsste stattdessen bei jedem Append gepflegt werden.

## Der Stream selbst

`ChatStream.Create` erzeugt die Item-Folge, die `TypedResults.ServerSentEvents` an den Browser
schreibt. Der Ablauf:

1. **Ein Heartbeat sofort.** `ServerSentEvents` schreibt nichts — nicht einmal die Statuszeile
   — bevor das erste Item geliefert wird. Ohne diesen Anschub wartete der Client in einem
   stillen Raum ein volles Heartbeat-Intervall, bevor `EventSource` den Zustand `CONNECTING`
   verlässt und `onopen` auslöst.
2. **Der Verlauf**, nach Offset sortiert und damit in der Reihenfolge, in der geschrieben
   wurde. Deshalb darf das Frontend blind anhängen.
3. **Die Schleife**: entweder die nächste Nachricht oder, nach 15 s Stille, ein Heartbeat.

### Heartbeats

Das Intervall ist 15 s: lang genug, um billig zu bleiben, kurz genug, um die verbreiteten 60 s
Idle-Timeout zu unterbieten. Heartbeats halten inaktive Verbindungen durch Proxys offen und
machen tote Gegenstellen sichtbar.

Der Event-Typ ist `ping` und ausdrücklich nicht der Vorgabetyp: `EventSource.onmessage` feuert
nur für `message`, der Browser verwirft diese Frames also, ohne sie je zu sehen.

Früher war das ein SSE-Kommentar (`": ping"`). `SseItem<T>` kann keinen Kommentar ausdrücken —
es trägt Daten, einen Event-Typ und eine ID, sonst nichts. Für dieses Frontend ist beides
gleich wirkungslos, und für jeden Client, der nach Event-Typ filtert statt jeden Frame zu
parsen, bleibt es folgenlos.

**Heartbeats tragen bewusst keine Event-ID.** Nach der SSE-Spezifikation lässt ein Frame ohne
`id`-Feld den Last-Event-ID-Puffer des Clients unangetastet. Würde man Heartbeats stempeln,
schöbe das den Wiederaufnahmepunkt über Nachrichten hinweg, die der Browser nie bekommen hat.

### Presence-Erneuerung huckepack auf dem Heartbeat

`GET /api/stream` nimmt zusätzlich einen `nickname`-Query-Parameter. Fehlt er oder besteht er
`ChatMessage.TryNormalizeNickname` nicht (leer, zu lang, reserviert, oder nur unter einer
unsichtbaren-Zeichen-Variante eines reservierten Namens), wird Presence für diese Verbindung
einfach übersprungen — kein Fehler für den ganzen Stream, weil Presence nur eine weiche
UX-Schranke hinter `POST /api/join` ist, keine Sicherheitsgrenze. Die Normalisierung selbst ist
aber keine weiche Schranke: ohne sie könnte dieser Pfad an `POST /api/join` vorbei unter einer
Zero-Width-Variante von `claude` Presence eintragen, siehe `ChatMessage.StripInvisibleCharacters`.

Statt eines eigenen Timers nutzt die Erneuerung der Präsenz-Reservierung denselben Rhythmus wie
der bestehende 15s-Heartbeat: `ChatStream.Create` prüft bei **jeder** Schleifeniteration, ob
seit der letzten Erneuerung mindestens `heartbeatInterval` vergangen ist — nicht nur im
Ping-Zweig. Ein voller Raum durchläuft die Schleife über echte Nachrichten, ohne je in den
15s-Timeout zu laufen; würde die Erneuerung an den Ping-Zweig gebunden, erneuerte ein solcher
Raum Presence nie. Die zeitbasierte Prüfung liefert in beiden Fällen dasselbe Verhalten: nie
öfter als einmal je Intervall in einem vollen Raum, nie seltener als einmal je Intervall in
einem stillen.

Details zum Presence-Topic selbst (Key, Tombstones, TTL, bewusst weicher Ausfall) stehen in
[kafka.md](kafka.md#presence-topic).

### Eine bekannte Grenze: `/api/join` und die Erneuerung sind nicht atomar

`POST /api/join` prüft die Reservierung und produziert sie in zwei getrennten Schritten, ohne
Sperre dazwischen; die Erneuerung in `ChatStream` prüft beim Auffrischen nicht erneut, ob die
Reservierung noch demselben Anrufer gehört. In einem seltenen Rennen kann ein Reconnect einen
Namen zurückerobern, den in der Zwischenzeit jemand anderes belegt hat. Bewusst nicht weiter
abgesichert — das TTL-Sicherheitsnetz heilt eine solche Kollision ohnehin innerhalb von
`PRESENCE_TTL_SECONDS` selbst, und diese App hat keine Anmeldung, gegen die eine stärkere
Garantie überhaupt etwas schützen würde.

### Wiederaufnahme

`Last-Event-ID` wird in `Program.cs` gelesen und als `afterOffset` durchgereicht.
`ChatHistory.Snapshot` liefert alles mit `Offset > afterOffset`. Kafka-Offsets beginnen bei 0,
also lässt `-1` alles durch — das ist der Wert für eine frische Verbindung.

Alles, was sich nicht parsen lässt, gilt als frische Verbindung. Den Raum noch einmal
auszuspielen ist die harmlose Antwort; ihn wegzulassen wäre es nicht.

## Rollierende Updates

Der Stream endet, sobald `IHostApplicationLifetime.ApplicationStopping` ausgelöst wird.

`RequestAborted` allein reicht dafür nicht — es feuert bei einem Rollout nie: Der Browser ist
noch da, und die Verbindung ist in Ordnung. Ohne `ApplicationStopping` heartbeatete der Stream
also die vollen 25 s des Shutdown-Timeouts aus einem endenden Pod weiter. Der Browser sähe
eine kerngesunde Verbindung zu einem Pod, der schon geht, und hätte keinen Anlass, zu der
längst bereitstehenden Replica zu wechseln.

Beide Abbruchgründe — „dieser Browser ist weg" und „dieser Pod geht weg" — werden über ein
verbundenes Token gleich behandelt.

Der Header `X-Accel-Buffering: no` wird zusätzlich gesetzt. `ServerSentEvents` setzt
`Content-Type`, `Cache-Control`, `Pragma` und `Content-Encoding` selbst und schaltet die
Antwortpufferung ab. `X-Accel-Buffering` ist ein nginx-spezifischer Hinweis, den es nicht
kennt. Er wirkt nur, wenn ein puffernder Proxy davorsteht — Caddy streamt SSE ungepuffert.
