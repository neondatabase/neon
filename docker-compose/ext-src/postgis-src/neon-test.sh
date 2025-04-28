#!/bin/bash
set -ex
cd "$(dirname "$0")"
if [[ ${PG_VERSION} = v17 ]]; then
  sed -i '/computed_columns/d' regress/core/tests.mk
fi
patch -p1 <postgis-no-upgrade-test.patch
trap 'echo Cleaning up; patch -R -p1 <postgis-no-upgrade-test.patch' EXIT
make installcheck-base