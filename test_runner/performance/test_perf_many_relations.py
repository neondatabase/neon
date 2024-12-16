

from pathlib import Path
import pytest
import os
from fixtures.compare_fixtures import PgCompare

def get_num_relations(default: int = 1000) -> list[int]:
    # We parametrize each run with scale specifying the number of wanted child partitions.
    # Databases are pre-created and passed through BENCHMARK_CONNSTR env variable.
    scales = os.getenv("TEST_NUM_RELATIONS", default=str(default))
    rv = []
    for s in scales.split(","):
        scale = int(s)
        rv.append(scale)
    return rv


@pytest.mark.parametrize("num_relations", get_num_relations())
def test_perf_many_relations(neon_with_baseline: PgCompare, num_relations: int):
    """
    Test creating many relations in a single database.
    We use partitioned tables with child tables, indexes and constraints to have a realistic schema.
    Also we include some common data types like text, uuid, timestamp, JSONB, etc.

    see many_relations/create_many_relations.sql
    """
    env = neon_with_baseline

    # prepare some base tables and the plpgsql procedures that we use to create the tables
    sql_file = Path(__file__).parent / "many_relations" / "create_many_relations.sql"
    env.pg_bin.run(["psql", env.pg.connstr(),"-f", str(sql_file)])
