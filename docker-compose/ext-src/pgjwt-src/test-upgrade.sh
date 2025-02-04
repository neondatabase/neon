#!/bin/sh
set -ex
cd "$(dirname ${0})"
patch -p1 <test-upgrade.patch
pg_prove test.sql