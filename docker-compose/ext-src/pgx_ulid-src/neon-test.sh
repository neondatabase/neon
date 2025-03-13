#!/bin/bash
set -ex
if [[ ${PG_VERSION/v/} -lt 17 ]]; then
  exit 0
fi
make installcheck