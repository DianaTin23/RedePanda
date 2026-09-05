# Shared helpers for the scripts in this repo. Sourced, never executed.
#
#   source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
#
# Each function here replaces a block that used to be copy-pasted into four or five scripts.

# Absolute path of the repository root, derived from this file's own location.
# Always call as REPO_ROOT="$(repo_root)": the subshell keeps the cd out of the caller.
repo_root() (
    cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
)

# Aborts with exit 2 unless every named tool is on PATH. Exit 2 means "usage or tooling error"
# throughout this repo, as distinct from 1, which means a check actually failed.
require_tool() {
    local tool
    for tool in "$@"; do
        if ! command -v "${tool}" >/dev/null 2>&1; then
            echo "${tool} not found on PATH. It is in the Nix dev shell: nix develop" >&2
            exit 2
        fi
    done
}

# Prints the calling script's own --help: line 2 down to the first non-comment line.
#
# Self-delimiting on purpose. These headers used to be printed with a hardcoded `sed -n '2,18p'`,
# so inserting a line into a header silently truncated or overran its own help output -- and the
# ranges then had to be documented in three other files to stay correct. The block now ends where
# the comments end, which is the only place it was ever supposed to end.
usage() {
    sed -e '1d' -e '/^[^#]/,$d' "$0"
    exit 0
}
