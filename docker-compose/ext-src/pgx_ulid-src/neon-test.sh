#!/bin/bash
set -ex
if [[ ${PG_VERSION/v/} -lt 17 ]]; then
  exit 0
fi
cd "$(dirname ${0})"
make installcheck