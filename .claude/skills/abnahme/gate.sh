#!/usr/bin/env bash
# The local gate: everything dotnet.yml and chart.yml check, run in one go before pushing.
#
#   .claude/skills/abnahme/gate.sh              # everything
#   .claude/skills/abnahme/gate.sh --chart-only # skip the .NET half (slow part)
#
# The chart rules themselves live in scripts/validate-chart.sh, which CI and the edit hook call
# too. This script is the .NET half plus that call, not a second transcription of the rules.
#
# A `dotnet test` without ContinuousIntegrationBuild=true silently rewrites the lock files
# instead of failing on drift, which is why the flag is not cosmetic here.
#
# This does not replace README section 12: the manual acceptance list needs a cluster, and CI
# does not have one either.
#
# Exit status: 0 when every step passed, 1 when one of them failed, 2 on a usage or tooling error.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../../../scripts/lib/common.sh"
cd "$(repo_root)" || exit 2

chart_only=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --chart-only) chart_only=1; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

require_tool helm kubeconform
[[ "${chart_only}" -eq 1 ]] || require_tool dotnet

failed=0
step() { printf '\n==> %s\n' "$1"; }
fail() { echo "    FAILED: $1" >&2; failed=1; }

if [[ "${chart_only}" -eq 0 ]]; then
    step "Dependencies restore against their committed lock files"
    ./scripts/check-repro.sh || fail "check-repro.sh"

    step "Test suite in locked mode"
    dotnet test -p:ContinuousIntegrationBuild=true || fail "dotnet test"

    step "Lock files unchanged by the test run"
    git diff --exit-code -- '*packages.lock.json' || fail "a lock file was rewritten -- see the diff above"
else
    echo "==> skipping the .NET half (--chart-only)"
fi

step "Chart: both HPA variants, the replicas coupling, and the release-file guard"
./scripts/validate-chart.sh || fail "validate-chart.sh"

echo
if [[ "${failed}" -eq 0 ]]; then
    echo "==> Everything that is checkable without a cluster passed."
    echo "    The manual list in README section 12 still needs a cluster."
else
    echo "==> The gate failed. Nothing above was pushed." >&2
fi
exit "${failed}"
