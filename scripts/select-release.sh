#!/usr/bin/env bash
# Prints the path of the release file the chart should be rendered with.
#
#   ./scripts/select-release.sh                    # newest committed, mtime as a fallback
#   ./scripts/select-release.sh --committed-only   # newest committed, no fallback (CI)
#
# The chart has no default image tag and refuses to render without one of these files, so
# every caller that renders the chart needs this answer first.
#
# Why git and not `ls -t`: a fresh clone writes every file in the same instant, so the mtimes
# are equal and `-t` orders them arbitrarily -- CI picked a release two versions old that way.
# `git log --diff-filter=A` asks which file was added last instead, which is stable everywhere.
# `-dirty` builds are gitignored and never committed, so they are filtered out of the fallback.
#
# And `command ls`, not `ls`: an `ls` alias on eza (set on at least one machine in this
# project) reads `-t` as an option *with an argument* and swallows the glob silently.
#
# Exit status: 0 with a path on stdout, 2 when there is no release file to name.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
cd "$(repo_root)" || exit 2

committed_only=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        --committed-only) committed_only=1; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

release="$(git log --diff-filter=A --name-only --format= -- 'deploy/releases/*.yaml' \
    | grep -v -- '-dirty\.' | grep . | head -1)"

if [[ -z "${release}" || ! -f "${release}" ]] && [[ "${committed_only}" -eq 0 ]]; then
    release="$(command ls -t deploy/releases/*.yaml 2>/dev/null | head -1)"
fi

if [[ -z "${release}" || ! -f "${release}" ]]; then
    echo "No release file in deploy/releases/. Build one: ./scripts/build-images.sh --release" >&2
    exit 2
fi

printf '%s\n' "${release}"
