#!/usr/bin/env bash
set -euo pipefail

# If you save this in your path under the name "cargo-zclippy" (or whatever
# name you like), then you can run it as "cargo zclippy" from the shell prompt.
#
# If your text editor has rust-analyzer integration, you can also use this new
# command as a replacement for "cargo check" or "cargo clippy" and see clippy
# warnings and errors right in the editor.
# In vscode, this setting is Rust-analyzer>Check On Save:Command

# NB: the CI runs the full feature powerset, so, it catches slightly more errors
# at the expense of longer runtime. This script is used by developers, so, don't
# do that here.

thisscript="${BASH_SOURCE[0]}"
thisscript_dir="$(dirname "$thisscript")"
CLIPPY_COMMON_ARGS="$( source .neon_clippy_args; echo "$CLIPPY_COMMON_ARGS")"
exec cargo clippy --all-features $CLIPPY_COMMON_ARGS
