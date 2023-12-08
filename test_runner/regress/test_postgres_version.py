from pathlib import Path

import pytest
from fixtures.neon_fixtures import PgBin
from fixtures.pg_version import PgVersion

# A list of expected Postgres versions and corresponding SHA of the commit
# Please update this list along with updating Postgres in vendor/ directory
EXPECTED_VERSIONS = {
    PgVersion.V15: ("15.3", "1984832c740a7fa0e468bb720f40c525b652835d"),
    PgVersion.V14: ("14.8", "1144aee1661c79eec65e784a8dad8bd450d9df79"),
}


@pytest.mark.xfail
def test_postgres_version(pg_bin: PgBin, pg_version: PgVersion):
    """Test that Postgres version matches the one we expect"""

    output_prefix = pg_bin.run_capture(["postgres", "--version"])
    stdout = Path(f"{output_prefix}.stdout")
    assert stdout.exists(), "postgres --version didn't print anything to stdout"

    with stdout.open() as f:
        output = f.read().strip()

    # `postgres --version` prints something like "postgres (PostgreSQL) 14.8 1144aee1661c79eec65e784a8dad8bd450d9df79".
    # We need only last 2 components of it — verison itself and SHA of the commit.
    _, version, sha = output.rsplit(" ", 2)
    msg = f"Unexpected Postgres {pg_version} version: `{output}`, please update EXPECTED_VERSIONS (in the test) if needed"
    assert (version, sha) == EXPECTED_VERSIONS[pg_version], msg
