#!/bin/sh
set -ex
cd "$(dirname "${0}")"
if [ -f Makefile ]; then
  make installcheck
fi
