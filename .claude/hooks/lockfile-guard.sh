#!/usr/bin/env bash
# Claude Code PostToolUse hook. Wired in .claude/settings.json for `Bash(dotnet *)`.
#
# `dotnet build`, `dotnet test` and `dotnet run` rewrite packages.lock.json when the resolved
# graph no longer matches it. They do not say so, and the rewrite is easy to commit without
# noticing -- the file that records what was tested quietly becomes a record of whatever
# resolved most recently. This makes that visible in the same turn it happens.
#
# Exit code 2 is what hands the message back to the model rather than only to the terminal.
# The real proof is scripts/check-repro.sh; this is only an early warning.
set -uo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
git rev-parse --git-dir >/dev/null 2>&1 || exit 0

drifted="$(git diff --name-only -- '*packages.lock.json')"
[[ -n "${drifted}" ]] || exit 0

{
    echo "A dotnet command rewrote lock files that are not staged:"
    echo "${drifted}" | sed 's/^/    /'
    echo
    echo "  Unintended -> discard:   git checkout -- '*packages.lock.json'"
    echo "  Deliberate -> the rewritten lock file belongs in the same commit as the version change."
    echo "  Either way, the check is:  ./scripts/check-repro.sh"
} >&2
exit 2
