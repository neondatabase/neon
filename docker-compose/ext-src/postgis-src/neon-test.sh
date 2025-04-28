#!/bin/bash
set -ex
cd "$(dirname "$0")"
if [[ ${PG_VERSION} = v17 ]]; then
  sed -i '/computed_columns/d' regress/core/tests.mk
fi
TMPDIR=$(mktemp -d /tmp/ext-test-XXXXXX)
cp regress/runtest.mk "${TMPDIR}"
sed -i '27,36d' regress/runtest.mk
trap 'echo Cleaning up; mv "${TMPDIR}/runtest.mk" regress/runtest.mk && rmdir "${TMPDIR}"' EXIT
make installcheck-base