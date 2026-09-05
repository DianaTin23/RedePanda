---
name: abnahme
description: Führt das vollständige lokale Gate aus, bevor gepusht wird — Lock-Dateien, Tests im locked mode, Chart in beiden HPA-Varianten samt Negativfall. Spiegelt .github/workflows/dotnet.yml und chart.yml.
disable-model-invocation: true
---

# Abnahme

Das lokale Gate. Es prüft genau das, was `dotnet.yml` und `chart.yml` prüfen — nur bevor der Push
draußen ist statt danach.

## Ausführen

```bash
.claude/skills/abnahme/gate.sh              # alles
.claude/skills/abnahme/gate.sh --chart-only # ohne die .NET-Hälfte (die langsame)
```

Läuft nur in der Dev-Shell (`nix develop`): das Skript braucht `dotnet`, `helm` und
`kubeconform`.

## Was es prüft

Die .NET-Hälfte: `./scripts/check-repro.sh`, dann `dotnet test
-p:ContinuousIntegrationBuild=true`, dann `git diff --exit-code -- '*packages.lock.json'`.
Danach die Chart-Hälfte: `./scripts/validate-chart.sh`.

Zwei Dinge, die die Skripte selbst nicht sagen können:

- **Das `ContinuousIntegrationBuild=true` ist nicht Kosmetik.** Ohne es schreibt `dotnet test`
  eine abweichende Lock-Datei **still neu**, statt an der Drift zu scheitern. Der `git
  diff`-Schritt danach ist der Befund, falls doch etwas umgeschrieben wurde.
- **Die Chart-Regeln stehen in `scripts/validate-chart.sh`**, nicht hier. Diese Datei
  beschreibt sie nicht noch einmal; sie ist der Grund, warum die frühere Fassung
  auseinanderdriften konnte.

## Danach

Grün heißt: was ohne Cluster prüfbar ist, ist geprüft. Die manuelle Abnahmeliste in **README
Abschnitt 13** bleibt davon unberührt — sie braucht einen Cluster, und CI hat auch keinen.

Wenn der Lock-Datei-Schritt anschlägt: die neu geschriebene Datei entweder verwerfen
(`git checkout -- '*packages.lock.json'`) oder, wenn die Versionsänderung beabsichtigt war,
zusammen mit ihr in denselben Commit legen.
