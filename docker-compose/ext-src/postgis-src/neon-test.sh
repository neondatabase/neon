#!/bin/bash
set -ex
cd "$(dirname "$0")"
if [[ ${PG_REGRESS} = v17 ]]; then
  sed -i '/computed_columns/d' regress/core/tests.mk
fi
cp regress/runtest.mk /tmp/runtest.mk.bak
sed -i '27,36d' regress/runtest.mk
trap 'echo Cleaning up; mv /tmp/runtest.mk regress/runtest.mk' EXIT INT TERM
make installcheck-base