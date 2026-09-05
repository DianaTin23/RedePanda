#!/usr/bin/env bash
# Claude Code PostToolUse hook. Wired in .claude/settings.json for Edit|Write.
#
# Renders the chart after every edit below deploy/helm/ and reports what broke. The rules are
# scripts/validate-chart.sh, the same script CI and /abnahme run -- this hook only decides *when*
# to run them. It used to carry its own copy of the checks, and that copy had drifted: it accepted
# any `replicas:` value where CI insisted on the one values.yaml names.
#
# --quick keeps it offline and fast: kubeconform fetches its schemas over the network, so schema
# validation stays in /abnahme and in CI rather than running on every keystroke.
#
# Exit code 2 hands the report back to the model.
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
command -v helm >/dev/null 2>&1 || exit 0
[[ -x scripts/validate-chart.sh ]] || exit 0

if ! report="$(./scripts/validate-chart.sh --quick 2>&1)"; then
    printf 'chart-guard: the chart does not hold after this edit.\n\n%s\n' "${report}" >&2
    exit 2
fi
exit 0
