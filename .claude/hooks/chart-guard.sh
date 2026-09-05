#!/usr/bin/env bash
# Claude Code PostToolUse hook. Wired in .claude/settings.json for Edit|Write.
#
# Renders the chart after every edit below deploy/helm/ and reports what broke. `helm lint` is
# deliberately not the gate here: Helm 4 reports a template `fail` as INFO, so only
# `helm template` actually aborts -- which is why the release-file guard is checked by rendering
# *without* one and expecting failure.
#
# Kept offline and fast on purpose: kubeconform fetches its schemas over the network, so schema
# validation stays in /abnahme and in CI (.github/workflows/chart.yml) rather than running on every
# keystroke. Exit code 2 hands the report back to the model.
set -uo pipefail

payload="$(cat)"
if command -v jq >/dev/null 2>&1; then
    file="$(printf '%s' "${payload}" | jq -r '.tool_input.file_path // .tool_response.filePath // empty')"
else
    # jq lives in the Nix dev shell. Without it the raw payload is matched instead, so the hook
    # still fires for chart edits rather than failing open on every edit in the repo.
    file="${payload}"
fi
case "${file}" in
    */deploy/helm/*) ;;
    *) exit 0 ;;
esac

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
CHART="deploy/helm/redetim"
command -v helm >/dev/null 2>&1 || exit 0
[[ -d "${CHART}" ]] || exit 0

# Which release file to validate against. Git knows which one was added last, which is the same
# question CI asks; a locally built release file is not committed yet, so fall back to mtime.
REL="$(git log --diff-filter=A --name-only --format= -- 'deploy/releases/*.yaml' 2>/dev/null \
    | grep -v -- '-dirty\.' | grep . | head -1)"
[[ -n "${REL}" && -f "${REL}" ]] || REL="$(command ls -t deploy/releases/*.yaml 2>/dev/null | head -1)"
if [[ -z "${REL}" || ! -f "${REL}" ]]; then
    echo "chart-guard: no release file under deploy/releases/ -- the chart cannot render." >&2
    exit 2
fi

problems=()

render() {
    helm template redetim "${CHART}" -n redetim -f "${REL}" "$@" 2>&1
}

if ! out="$(render)"; then
    problems+=("Rendering fails without the HPA:"$'\n'"${out}")
fi

# Rendered separately on purpose: nothing validates backend-hpa.yaml otherwise.
if ! out="$(render --set backend.autoscaling.enabled=true)"; then
    problems+=("Rendering fails with backend.autoscaling.enabled=true:"$'\n'"${out}")
fi

# replicas belongs to the HPA or to the chart, never to both.
with_hpa="$(helm template redetim "${CHART}" -f "${REL}" --set backend.autoscaling.enabled=true \
    --show-only templates/backend.yaml 2>/dev/null | grep -c '^  replicas:')"
if [[ "${with_hpa}" != "0" ]]; then
    problems+=("backend.yaml renders 'replicas' while the HPA owns it -- Helm and the autoscaler will fight.")
fi
if ! helm template redetim "${CHART}" -f "${REL}" --show-only templates/backend.yaml 2>/dev/null \
    | grep -q '^  replicas:'; then
    problems+=("backend.yaml renders no 'replicas' even though no HPA is active.")
fi

# The chart must refuse to render without a release file, or `helm rollback` stops meaning anything.
if helm template redetim "${CHART}" >/dev/null 2>&1; then
    problems+=("The chart rendered without a release file -- the tag guard is gone.")
fi

[[ ${#problems[@]} -eq 0 ]] && exit 0

{
    echo "chart-guard (validated against ${REL}):"
    for p in "${problems[@]}"; do
        echo
        echo "${p}" | sed 's/^/    /'
    done
} >&2
exit 2
