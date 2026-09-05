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

## Was es prüft, und warum genau so

1. **`./scripts/check-repro.sh`** — restaurieren alle vier Projekte im locked mode gegen ihre
   Lock-Dateien.
2. **`dotnet test -p:ContinuousIntegrationBuild=true`** — die Suite. Das Flag ist nicht Kosmetik:
   ohne es schreibt `dotnet test` eine abweichende Lock-Datei **still neu**, statt an der Drift
   zu scheitern.
3. **`git diff --exit-code -- '*packages.lock.json'`** — hat der Testlauf trotzdem etwas
   umgeschrieben, ist das hier der Befund.
4. **Chart, ohne HPA** — `helm lint` plus `helm template | kubeconform -strict`.
5. **Chart, mit `backend.autoscaling.enabled=true`** — muss **separat** gerendert werden, sonst
   validiert niemand `backend-hpa.yaml`.
6. **`replicas`-Kopplung** — das Feld gehört dem HPA oder dem Chart, nie beiden. Mit HPA darf
   `backend.yaml` es nicht rendern, ohne HPA muss es `replicas: 2` rendern.
7. **Rendern ohne Release-Datei muss scheitern** — der Tag-Guard. `helm lint` fängt das
   **nicht**: Helm 4 stuft ein `fail` im Template auf INFO herab, nur `helm template` bricht
   wirklich ab.

## Danach

Grün heißt: was ohne Cluster prüfbar ist, ist geprüft. Die manuelle Abnahmeliste in **README
Abschnitt 13** bleibt davon unberührt — sie braucht einen Cluster, und CI hat auch keinen.

Wenn Schritt 3 anschlägt: die neu geschriebene Lock-Datei entweder verwerfen
(`git checkout -- '*packages.lock.json'`) oder, wenn die Versionsänderung beabsichtigt war,
zusammen mit ihr in denselben Commit legen.
