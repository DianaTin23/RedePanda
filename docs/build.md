# Build, Images, Reproduzierbarkeit

Ein Build soll zweimal dasselbe ergeben, und ein Release soll benennbar sein. Beides ist hier
ohne CI umgesetzt, was den Aufwand von der Automatik in die Konstruktion verschiebt: Was nicht
geprüft werden kann, wird stattdessen unmöglich gemacht.

Die Befehle stehen in README Abschnitt 4 und 6. Hier steht, warum sie so aussehen.

## Zentrale Build-Konfiguration

Vier Dateien im Wurzelverzeichnis gelten für alle Projekte:

| Datei | Rolle |
|---|---|
| `Directory.Build.props` | `TargetFramework`, `Nullable`, `TreatWarningsAsErrors`, Lockfile-Schalter |
| `Directory.Packages.props` | Central Package Management — jede NuGet-Version steht hier |
| `NuGet.config` | welche Feeds befragt werden dürfen |
| `global.json` | die SDK-Version |

`TargetFramework` wird **hier** angehoben, nicht in den einzelnen `.csproj`. Die `.csproj`
tragen entsprechend blanke `<PackageReference Include="..." />` ohne Version.

`TreatWarningsAsErrors` steht auf `true`, und es gibt im ganzen Repository keine einzige
Ausnahme: kein `#pragma warning`, kein `[SuppressMessage]`, kein `NoWarn`.

### `NuGet.config` und das `<clear />`

Ein Restore darf nicht davon abhängen, welche Feeds zufällig auf der Maschine eingerichtet
sind. `<clear />` verwirft alles aus `~/.nuget/NuGet/NuGet.Config` und aus dem maschinenweiten
Speicher, sodass nur nuget.org befragt wird.

Ohne das pinnen die Lockfiles zwar weiterhin, *welche* Paketversionen benutzt werden, aber
nicht, woher sie kommen. Ein lokaler oder firmeninterner Feed, der unter derselben ID und
Version ein anderes Artefakt ausliefert, erfüllte das Lockfile anstandslos.

## Lockfiles, und wann sie tatsächlich etwas erzwingen

`Directory.Packages.props` pinnt, was angefordert wird. Über den transitiven Graphen, den diese
Pakete mitziehen, sagt es nichts. Das tun die `packages.lock.json`: Sie halten den vollständig
aufgelösten Graphen samt Inhalts-Hashes fest und liegen neben jedem Projekt.

Hier liegt der Haken. Ein gewöhnliches `dotnet restore` **schreibt das Lockfile neu**, wenn
eine Version sich geändert hat. `dotnet build`, `dotnet test` und `dotnet run` tun das
ebenfalls. Sie beschweren sich nicht, und die Neufassung ist leicht unbemerkt mitzucommitten —
die Datei, die aufzeichnen soll, was getestet wurde, wird damit still zur Aufzeichnung dessen,
was zuletzt aufgelöst wurde.

Nur im *locked mode* wird aus derselben Abweichung ein Fehlschlag. Genau das ist der Wert der
Datei.

Locked mode ist an `ContinuousIntegrationBuild` gebunden statt schlicht eingeschaltet, weil er
falsch ist, solange eine Abhängigkeit absichtlich geändert wird: Er verweigerte genau den
Restore, der das Lockfile aktualisieren soll.

```sh
dotnet test -p:ContinuousIntegrationBuild=true
```

Bis diese Bedingung existierte, war der einzige Ort mit locked mode der Container-Build — und
der schließt `tests/` über `.dockerignore` aus. Ein gewöhnliches `dotnet test` schrieb also drei
der vier Lockfiles still neu, statt zu scheitern. Die einzige Absicherung dieses Repositories
gegen ein ungeprüftes transitives Paket-Upgrade war ausgerechnet in dem Befehl aus, den alle
ausführen.

`scripts/check-repro.sh` macht das über alle vier Projekte. Es prüft zusätzlich die Prüfsummen
aller Lockfiles vor und nach dem Lauf: Ein Restore im locked mode schreibt die Datei trotzdem
neu, wenn er zu dem Schluss kommt, dass er darf.

Das Skript läuft nicht von selbst. README Abschnitt 13 listet es, Abschnitt 14 nennt das
fehlende CI als bekannte Einschränkung.

### Der doppelte Bindestrich

In `Directory.Build.props` steht ein Hinweis, der wie eine Marotte aussieht und keine ist: **XML
verbietet `--` innerhalb eines Kommentars.**

Eine frühere Fassung des Blocks benutzte ihn als Gedankenstrich. MSBuilds Antwort darauf war,
ein leeres `TargetFramework` aus einer völlig anderen Datei zu melden. Der Weg von dieser
Fehlermeldung zurück zur Ursache ist lang genug, dass die Warnung dort stehen bleibt.

## Digest-Pinning

Jedes Image aus einer Registry ist per Digest gepinnt, nicht nur per Tag. Ein Tag kann auf
anderen Inhalt umgehängt werden, ein Digest nicht. Der Tag bleibt daneben stehen, damit die
Version lesbar ist — aufgelöst wird über den Digest.

Gepinnt wird der **Manifest-List-Digest**, nicht der plattformspezifische. Der Digest eines Tags
ist der sha256 der Manifest-Bytes, die die Registry dafür zurückgibt; bei einem
Multi-Architektur-Image ist das der Index. Genau diesen Index-Digest löst ein `docker pull` auf
jeder Architektur auf. Fragte man skopeo stattdessen nach einem plattformspezifischen Manifest,
entstünde ein Pin, der nur auf amd64 funktioniert.

`scripts/check-digests.sh` meldet, wenn ein Tag upstream weitergewandert ist. Es schreibt
**nichts** um: Es druckt die Ersatzzeile und überlässt die Änderung einem Menschen, weil es hier
kein CI gibt, das eine fehlerhafte automatische Umschreibung eines Dockerfiles oder von
`values.yaml` auffinge.

Die Dateiliste im Skript ist ausgeschrieben und nicht geglobbt. Ein neuer Pin in einer Datei,
die niemand dort eingetragen hat, bleibt damit ungeprüft — das ist sichtbar. Die Alternative
wäre, eine unbeteiligte Datei nach sha256-Zeichenketten abzugrasen.

Eine fehlgeschlagene Abfrage — kein Netz, oder eine Registry, die anonyme Zugriffe drosselt —
ist berichtenswert, aber nicht fatal. Sie wird geschluckt, und der Aufrufer behandelt leere
Ausgabe als „ungeprüft". Andernfalls ergäbe der Hash der leeren Zeichenkette einen völlig
gültig aussehenden Digest, der zu nichts passt.

Namen werden vorher normalisiert. `alpine:3.23` und `docker.io/library/alpine:3.23` benennen
dasselbe Image, aber nur die zweite Form ist für skopeo eindeutig — es kennt den
Vorgabe-Namespace des Daemons nicht. Angewandt werden dieselben zwei Regeln wie in jeder
Container-Engine: ein Name ohne Schrägstrich lebt in `docker.io/library`, und ein einteiliges
Präfix, das kein Hostname ist (kein Punkt, kein Port), ist ein Docker-Hub-Benutzer und keine
Registry.

### Broker-Parität

Der Broker in der Compose-Datei und der Broker im Chart sollen dasselbe Image sein, bis auf den
Digest. Das *ist* Dev/Prod-Parität für den einen geteilten Backing Service dieses Repositories —
und ein Kommentar, der sie behauptet, ist keine Prüfung. Dass zwei Leute die eine Datei
aktualisieren und die andere nicht, ist der gewöhnliche Weg, auf dem so etwas auseinanderläuft.

Die Prüfung läuft **vor** den Registry-Abfragen, weil sie kein Netz braucht: Ein Offline-Lauf
bekommt diese Antwort trotzdem, und eine Abweichung hier machte den Drift-Bericht danach ohnehin
irreführend.

Sie deckt jede Stelle außerhalb des Charts ab, die den Broker benennt. Lange Zeit war es nur die
erste davon — genau die Fehlerart, die diese Prüfung eine Ebene höher verhindern soll: Eine
Paritätsprüfung, die einige der Kopien abdeckt, liest sich exakt wie eine, die alle abdeckt.

## Die Images

### Backend und Konsolenclient

Beide bauen aus dem Wurzelverzeichnis, weil beide `RedeTim.Contracts` referenzieren. Nur das
Frontend hat einen eigenen Kontext.

**Restore vor dem Kopieren der Quellen**, damit eine Codeänderung die Paketschicht nicht
entwertet. Die Wurzelkonfiguration kommt zuerst: Ohne `Directory.Packages.props` schlägt der
Restore bei zentraler Paketverwaltung sofort fehl, und `NuGet.config` verhindert den Rückfall
auf die Feeds des Build-Hosts.

**Die Lockfiles reisen mit ihrer `.csproj`.** `--locked-mode` liest sie während desselben
Restores; sie später mit den Quellen zu kopieren wäre zu spät. `--locked-mode` macht aus einem
Lockfile, das nicht mehr zum Projekt passt, einen Build-Fehler (NU1004) statt eines stillen
Upgrades.

**Das Runtime-Image ist Debian-basiert, und das ist keine Bequemlichkeit.** `Confluent.Kafka`
bringt native librdkafka-Assets mit, die gegen glibc gebaut sind:

- Eine `-alpine`-Basis (musl) kann sie über den RID-Graphen nicht auflösen.
- Eine `-chiseled`-Basis hat keine verlässliche `/etc/os-release` für die Plattformprobe des
  Loaders.

Beides scheitert beim ersten `ConsumerBuilder.Build()` — zur **Laufzeit**, nicht beim Bauen.

**SDK und Runtime werden unterschiedlich gepinnt.** Das SDK-Image hängt am selben Feature-Band
wie die Nix-Dev-Shell (`flake.nix`, `global.json`): `rollForward` ist `latestPatch`, ein
10.0.4xx-Image erfüllte eine Anfrage nach 10.0.302 also nicht. Das Runtime-Image bleibt beim
Major-Stream `10.0`, weil `global.json` nur das SDK einschränkt und eine veröffentlichte
net10.0-Anwendung auf jeder 10.0.x-Runtime läuft. Gepinnt wird ohnehin über den Digest.

**`USER $APP_UID`** — eine numerische UID aus dem Basis-Image. Kubernetes' `runAsNonRoot`-Prüfung
lehnt Images ab, deren `USER` ein Name statt einer Zahl ist.

Das Frontend-Image ist in [frontend.md](frontend.md#das-image) beschrieben; es hat eigene
Gründe.

## `scripts/build-images.sh`

### Die Version

Sie kommt aus `appVersion` in `Chart.yaml` plus dem Commit. Bei einem unsauberen Arbeitsbaum
kommt ein Hash des uncommitteten Inhalts dazu.

Ein schlichtes `-dirty` wäre wieder veränderlich: Jede Bearbeitung landete unter demselben Tag.
Den Inhalt zu hashen hält ein Tag an einen Baum. `git diff HEAD` deckt Änderungen und Löschungen
ab; die Dateiliste ergänzt den Inhalt nicht verfolgter Dateien, den kein Diff gegen HEAD sehen
kann.

Dabei `-o` und nicht `-mo`: Eine gelöschte verfolgte Datei ist für git „modified", `-m` listete
also einen Pfad, den `sha256sum` danach nicht öffnen konnte. `xargs` beendete sich mit 123 und
riss unter `set -e` den ganzen Build ab, bevor irgendetwas gebaut war. Löschungen stecken
ohnehin schon im Diff.

Der Konsolenclient bekommt denselben Tag wie die anderen beiden. Das ist Absicht: Der
Admin-Prozess muss derselbe Build sein wie die Anwendung, die er verwaltet — genau darum ist er
kein Shell-Skript in einem fremden Image mehr.

### Die Release-Datei ist das Release

`deploy/releases/<version>.yaml` benennt die exakten Images, die ein Build erzeugt hat. Sie an
Helm zu übergeben ist das, was diesen Build an die Konfiguration des Charts bindet, und sie ist
das, was ein späteres `helm rollback` wiederherstellt.

Sie trägt die Kopfzeile `# Generated by scripts/build-images.sh -- do not edit.` Diese Zeile
stammt aus einem Heredoc im Skript und ist der einzige maschinengeschriebene Kommentar im
Repository.

Committen — außer die Version sagt „dirty".

`--description` beim `helm upgrade` ist das einzige Feld pro Revision, an das ein Wert
überhaupt gelangt: `helm history` liest seine Spalte APP VERSION aus `Chart.yaml`, die auf jeder
Revision identisch und damit zum Unterscheiden zweier Releases nutzlos ist.

### Warum es kein gerendertes Manifest mehr gibt

`deploy/k8s/rendered.yaml` wurde einmal aus `helm template` geschrieben, damit sich das Release
auch ohne Helm installieren ließ. Es kostete mehr, als es einbrachte:

- Eine gerenderte Datei kann die `fail`-Prüfungen des Charts zur Renderzeit nicht mitnehmen —
  wer Helm übersprang, übersprang jede Kontrolle, die eine Fehlkonfiguration laut macht.
- TLS hat keinen Ausschalter, jedes Rendern mintete also eine CA und vier private Schlüssel und
  legte sie hier ab.
- `helm template` rendert `.Release.Revision` immer als `1`, der Topic-Job behielt damit einen
  Namen, und das zweite `kubectl apply` scheiterte an einem unveränderlichen Feld.
- Nichts bemerkte Drift, und es driftete — um fünf fehlende Secrets und mehrere hundert Zeilen.

Helm ist der Installationsweg. Das Release-Artefakt ist `deploy/releases/<version>.yaml`.

Ein wichtiger Nachsatz: Die Aufgabenstellung verlangt Manifeste, und **Templates *sind*
Manifeste**.

### Laden in einen lokalen Cluster

`kind load docker-image` liest den *Docker*-Speicher, den podman nicht füllt. Der Weg über ein
Archiv ist die unterstützte Route für ein mit podman gebautes Image.

Das Umtaggen davor ist nicht kosmetisch. Podman legt ein lokal gebautes Image unter
`localhost/<name>` ab, und `podman save` schreibt diesen Namen ins Archiv — der Node hielte also
`localhost/redetim-backend:<tag>`. Das Chart fragt nach dem blanken
`redetim-backend:<tag>`, was containerd zu `docker.io/library/redetim-backend:<tag>`
normalisiert: ein Name, den das Archiv nie trug. Der kubelet tut daraufhin das Einzige, was ihm
bleibt, und versucht von Docker Hub zu ziehen — `ImagePullBackOff` für ein Image, das
nachweislich schon auf dem Node liegt.

Unter dem vollqualifizierten Namen zu speichern bringt beide zur Deckung. `docker save` hängt
kein Präfix an, der andere Zweig braucht davon nichts.

Der minikube-Zweig hat dasselbe Problem und dieselbe Behebung, ist hier aber **nicht** auf einem
echten Cluster erprobt worden — es gibt kein minikube auf den Entwicklungsmaschinen.

Wird `podman` und `docker` beides gefunden, gewinnt podman: Auf den Maschinen dieses Projekts
ist `docker` ohnehin oft ein podman-Shim, und ausdrücklich zu sein erspart Überraschungen
darüber, in welchem Speicher das Image landet.

## Die `--help`-Köpfe der Skripte

Drei Skripte geben ihren eigenen Kommentarkopf als Hilfetext aus:

| Skript | Zeile | Bereich |
|---|---|---|
| `scripts/build-images.sh` | 30 | `sed -n '2,14p' "$0"` |
| `scripts/check-digests.sh` | 16 | `sed -n '2,9p' "$0"` |
| `scripts/check-repro.sh` | 27 | `sed -n '2,20p' "$0"` |

Diese Kopfblöcke sind **Code**, kein Kommentar. Eine gelöschte oder eingefügte Zeile verschiebt
die Ausgabe lautlos, und nichts prüft das. Wer dort etwas ändert, muss die Zeilenbereiche
mitziehen — oder besser: den Umfang gleich lassen.

## skopeo auf NixOS

`check-digests.sh` legt sich eine minimale `registries.conf` an. skopeo verweigert den Start,
wenn `/etc/containers/registries.conf` im alten v1-Format vorliegt, was manche Distributionen —
NixOS darunter — für podman noch installieren.

Keine der Abfragen hängt an Registry-Suchpfaden, `normalise_ref` qualifiziert jede Referenz
selbst. Die Probe zielt bewusst auf einen geschlossenen lokalen Port: Sie scheitert so oder so,
interessant ist nur, *welcher* Fehler zurückkommt, und sie kostet damit keine Registry-Anfrage.

Ihr Status wird verworfen und nur die Meldung gelesen — sie in `grep` zu pipen risse über
`pipefail` das ganze Skript mit.
