#!/bin/sh
set -ex
cd "$(dirname "$0")"
patch -p1 <"postgis-common-v${PG_VERSION}.patch"
trap 'echo Cleaning up; patch -R -p1 <postgis-common-v${PG_VERSION}.patch' EXIT
make installcheck-base