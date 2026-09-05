---
name: release
description: Schneidet ein Release — prüft die Vorbedingungen und löst den workflow_dispatch auf main aus, der die Images baut, nach ghcr.io pusht und die Release-Datei zurückcommittet.
disable-model-invocation: true
---

# Release

Ein Release ist hier ein **identifizierbarer Akt**, keine Nebenwirkung eines Merges. Deshalb
ist `.github/workflows/release.yml` ein eigener Workflow, der nur an `workflow_dispatch`
hängt und an keinem Push, und deshalb verweigert `build-images.sh` einen unsauberen Baum.

Der Tag wird **abgeleitet, nicht gewählt**: `appVersion` aus `Chart.yaml` plus kurzer Commit
(`0.1.0-g103b98b`), bei unsauberem Baum zusätzlich ein Inhalts-Hash. Ein Tag benennt damit genau
einen Build — das ist es, was `helm rollback` ein echtes Image zurückholen lässt statt desselben
mutierbaren Namens.

## Vorbedingungen — der Reihe nach prüfen

1. **Auf `main`, aktuell, sauber.** Der Job läuft nur auf `main`; ein unsauberer Baum wird
   abgelehnt, in CI sofort und in `build-images.sh` sonst erst vier Minuten später.

   ```bash
   git status --porcelain      # muss leer sein
   git rev-parse --abbrev-ref HEAD
   git fetch origin && git status -sb | head -1
   ```

2. **Das Gate ist grün.** `/abnahme` — oder mindestens die Gewissheit, dass der letzte
   CI-Lauf auf diesem Commit durchlief. `release.yml` ruft `dotnet.yml` und `chart.yml`
   ohnehin selbst auf und hängt per `needs` daran — der Release-Job startet nicht, wenn eines
   davon rot ist.

3. **Soll `appVersion` steigen?** Der Tag erbt sie aus `deploy/helm/redetim/Chart.yaml`. Eine
   Anhebung ist eine eigene, vorher gemergte Änderung — nicht Teil des Release-Laufs.

## Auslösen

```bash
gh workflow run release.yml --ref main
gh run watch "$(gh run list --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
```

Was der Job dann tut:

1. `dotnet.yml` und `chart.yml` laufen als aufgerufene Workflows und müssen grün sein (`needs`).
2. Arbeitsbaum-Prüfung, Login an `ghcr.io`.
3. `./scripts/build-images.sh --push` — baut die drei Images unter dem abgeleiteten Tag, pusht
   nach `ghcr.io/dianatin23/redetim-*` und schreibt `deploy/releases/<version>.yaml`.
4. Die Release-Datei wird als Artefakt hochgeladen — **bevor** committet wird, damit sie nicht
   verloren ist, falls `main` inzwischen weitergelaufen ist.
5. Commit der Release-Datei zurück nach `main`, ohne `--force`.

## Danach

```bash
git pull                                    # holt den Release-Commit
REL=$(./scripts/select-release.sh)
helm upgrade --install redetim deploy/helm/redetim -n redetim -f "$REL"
```

Schlägt Schritt 5 fehl, weil `main` weitergelaufen ist: die Images sind gepusht, die
Release-Datei liegt als Artefakt am Run. Herunterladen, committen, pushen — **nicht** neu bauen,
sonst entsteht ein zweiter Tag für denselben Stand.

## Was hier nicht passiert

- **Kein lokaler Push.** `./scripts/build-images.sh --push` von Hand umgeht die grüne
  Vorbedingung und pusht unter denselben Namen. Lokal baut man ohne Schalter, README
  Abschnitt 6.
- **Kein `rendered.yaml` im Repo.** Helm ist der einzige Installationsweg; ein gerendertes
  Manifest mintete bei jedem Lauf eine CA samt vier privaten Schlüsseln.
