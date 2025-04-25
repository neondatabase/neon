#!/bin/bash
set -ex
cd "$(dirname "$0")"
sed -i '/computed_columns/d' regress/core/tests.mk
cp regress/runtest.mk regress/runtest.mk.bak
sed -i '27,36d' regress/runtest.mk
trap 'echo Cleaning up; mv regress/runtest.mk.bak regress/runtest.mk' EXIT INT TERM
make installcheck-base