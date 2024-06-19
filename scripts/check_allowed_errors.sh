#!/usr/bin/env bash

set -eu

HELPER_DIR="$(dirname "${BASH_SOURCE[0]}")"
SCRIPT="test_runner/fixtures/pageserver/allowed_errors.py"

# first run to understand all of the errors:
#
# example: ./scripts/check_allowed_errors.sh -i - < pageserver.log
# example: ./scripts/check_allowed_errors.sh -i pageserver.log
#
# then edit the test local allowed_errors to the
# test_runner/fixtures/pageserver/allowed_errors.py, then re-run to make sure
# they are handled.
#
# finally revert any local changes to allowed_errors.py.
poetry run python3 "$HELPER_DIR/../$SCRIPT" $*
