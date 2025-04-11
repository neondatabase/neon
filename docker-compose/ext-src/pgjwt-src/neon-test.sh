#!/bin/bash
set -ex
cd "$(dirname "${0}")"
dropdb --if-exists contrib_regression
createdb contrib_regression
pg_prove -d contrib_regression test.sql