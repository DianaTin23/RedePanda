#!/usr/bin/env bash
# Checks that this tree still builds from exactly the dependencies it recorded.
#
#   ./scripts/check-repro.sh
#
# Locked mode is off by default because a deliberate dependency change *should* rewrite the lock
# file; this script turns it on. Why that matters: docs/build.md.
#
# CI runs this on every push and pull request (.github/workflows/dotnet.yml). README section 13
# lists it as well; run it by hand before cutting a release from a machine.
#
# Exit status: 0 when every project restores against its committed lock file, 1 when one of them
# has drifted, 2 on a usage or tooling error.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
REPO_ROOT="$(repo_root)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

require_tool dotnet

# Read from the solution rather than repeated here: a project added to RedeTim.sln but not
# to a second list in this file would have gone unchecked, which is exactly the drift this
# script exists to catch. Still one restore per project, so the report names which one.
mapfile -t PROJECTS < <(cd "${REPO_ROOT}" && dotnet sln list | grep "[.]csproj")
if [[ "${#PROJECTS[@]}" -eq 0 ]]; then
    echo "Could not read the project list from RedeTim.sln" >&2
    exit 2
fi

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
