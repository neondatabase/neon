#!/bin/bash
set -euo pipefail
# GNU coreutil's `install -C` always overrides the destination if the source
# is not a regular file, which is the case with lots of headers symlinked into
# the build directory by `./configure`. That causes Rust's Cargo to think that
# Postgres headers have been updated after `make` call even if no files have been
# touched. That causes long recompilation of `postgres_ffi` and all dependent
# packages. To counter that, we handle a special case here: do not copy the file
# if its content did not change. We only handle a single case where `install`
# installs a single file with a specific set of arguments, the rest does not
# matter in our configuration.
#
# Such behavior may be incorrect if e.g. permissions have changed, but it should
# not happen during normal Neon development that often, and rebuild should help.
#
# See https://github.com/neondatabase/neon/issues/1873
if [ "$#" == "5" ]; then
  if [ "$1" == "-C" ] && [ "$2" == "-m" ] && [ "$3" == "644" ]; then
    if [ -e "$5" ] && diff -q "$4" "$5" >/dev/null 2>&1; then
      exit 0
    fi
  fi
fi
install "$@"
