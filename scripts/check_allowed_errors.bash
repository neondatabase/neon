#!/bin/env bash

set -eu

HELPER_DIR="$(dirname "${BASH_SOURCE[0]}")"
SCRIPT="test_runner/fixtures/pageserver/allowed_errors.py"

# example: ./scripts/check_allowed_errors -i - < pageserver.log
# example: ./scripts/check_allowed_errors -i pageserver.log
poetry run python3 "$HELPER_DIR/../$SCRIPT" $*
