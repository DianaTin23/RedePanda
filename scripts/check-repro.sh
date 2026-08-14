#!/usr/bin/env bash
# Checks that this tree still builds from exactly the dependencies it recorded.
#
#   ./scripts/check-repro.sh
#
# Exit status: 0 when every project restores against its committed lock file, 1 when one of them
# has drifted, 2 on a usage or tooling error.
#
# Why this is a script rather than a habit: `dotnet build`, `dotnet test` and `dotnet run` all
# *rewrite* a lock file when the resolved graph no longer matches it. They do not complain, and the
# rewrite is easy to commit without noticing -- so the file that is supposed to be the record of
# what was tested quietly becomes a record of whatever resolved most recently. Locked mode turns
# that same mismatch into a failure, which is the whole value of having the file.
#
# It is not on by default in Directory.Build.props because a dependency change *should* rewrite the
# lock file; see the comment there. This script sets ContinuousIntegrationBuild=true, which is what
# switches locked mode on.
#
# This repository has no CI, so nothing runs this on its own. README section 13 lists it, and
# section 14 names the absence of CI as a known limitation.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if ! command -v dotnet >/dev/null 2>&1; then
    echo "dotnet not found on PATH. It is in the Nix dev shell: nix develop" >&2
    exit 2
fi

PROJECTS=(
    "src/RedePanda.Contracts/RedePanda.Contracts.csproj"
    "src/RedePanda.Backend/RedePanda.Backend.csproj"
    "src/RedePanda.ChatClient/RedePanda.ChatClient.csproj"
    "tests/RedePanda.Backend.Tests/RedePanda.Backend.Tests.csproj"
)

before="$(cd "${REPO_ROOT}" && find . -name packages.lock.json -not -path './**/bin/*' -not -path './**/obj/*' -print0 \
    | sort -z | xargs -0 sha256sum | sha256sum)"

failed=0
for project in "${PROJECTS[@]}"; do
    printf '==> %s\n' "${project}"
    if dotnet restore "${REPO_ROOT}/${project}" --locked-mode >/dev/null 2>&1; then
        echo "    ok"
    else
        echo "    DRIFT: the resolved packages no longer match packages.lock.json." >&2
        echo "    Review the change, then re-record it with:" >&2
        echo "      dotnet restore ${project} --force-evaluate" >&2
        failed=1
    fi
done

after="$(cd "${REPO_ROOT}" && find . -name packages.lock.json -not -path './**/bin/*' -not -path './**/obj/*' -print0 \
    | sort -z | xargs -0 sha256sum | sha256sum)"

echo
if [[ "${before}" != "${after}" ]]; then
    echo "==> A lock file was rewritten during this check, which locked mode should have prevented." >&2
    echo "    Inspect: git diff -- '*packages.lock.json'" >&2
    failed=1
fi

if [[ "${failed}" -eq 0 ]]; then
    echo "==> ${#PROJECTS[@]} projects restore against their committed lock files"
else
    echo "==> Dependencies have drifted from what was recorded." >&2
fi
exit "${failed}"
