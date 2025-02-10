#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
pg_prove -d contrib_regression test.sql