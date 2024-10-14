from __future__ import annotations

import json
import re
from pathlib import Path

from fixtures.neon_fixtures import PgBin
from fixtures.pg_version import PgVersion


def test_postgres_version(base_dir: Path, pg_bin: PgBin, pg_version: PgVersion):
    """Test that Postgres version matches the one we expect"""

    with (base_dir / "vendor" / "revisions.json").open() as f:
        expected_revisions = json.load(f)

    output_prefix = pg_bin.run_capture(["postgres", "--version"], with_command_header=False)
    stdout = Path(f"{output_prefix}.stdout")
    assert stdout.exists(), "postgres --version didn't print anything to stdout"

    with stdout.open() as f:
        output = f.read().strip()

    # `postgres --version` prints something like "postgres (PostgreSQL) 15.6 (85d809c124a898847a97d66a211f7d5ef4f8e0cb)".
    # beta- and release candidate releases would use '17beta1' and '18rc2' instead of .-separated numbers.
    pattern = (
        r"postgres \(PostgreSQL\) (?P<version>\d+(?:beta|rc|\.)\d+) \((?P<commit>[0-9a-f]{40})\)"
    )
    match = re.search(pattern, output, re.IGNORECASE)
    assert match is not None, f"Can't parse {output} with {pattern}"

    version = match.group("version")
    commit = match.group("commit")

    assert (
        pg_version.v_prefixed in expected_revisions
    ), f"Released PostgreSQL version `{pg_version.v_prefixed}` doesn't exist in `vendor/revisions.json`, please update it if these changes are intentional"
    msg = f"Unexpected Postgres {pg_version} version: `{output}`, please update `vendor/revisions.json` if these changes are intentional"
    assert [version, commit] == expected_revisions[pg_version.v_prefixed], msg
