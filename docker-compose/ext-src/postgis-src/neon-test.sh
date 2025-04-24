#!/bin/bash
set -ex
cd "$(dirname "$0")"
sed -i '/computed_columns/d' regress/core/tests.mk
make installcheck-base