from __future__ import annotations

import pytest
from fixtures.compare_fixtures import PgCompare

from performance.test_perf_pgbench import PgBenchLoadType, get_durations_matrix, run_test_pgbench


# The following test runs on an existing database that has pgvector extension installed
# and a table with 1 million embedding vectors loaded and indexed with HNSW.
#
# Run this pgbench tests against an existing remote Postgres cluster with the necessary setup.
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote_pgvector_hnsw(remote_compare: PgCompare, duration: int):
    run_test_pgbench(remote_compare, 1, duration, PgBenchLoadType.PGVECTOR_HNSW)


# The following test runs on an existing database that has pgvector extension installed
# and a table with 1 million embedding vectors loaded and indexed with halfvec.
#
# Run this pgbench tests against an existing remote Postgres cluster with the necessary setup.
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote_pgvector_halfvec(remote_compare: PgCompare, duration: int):
    run_test_pgbench(remote_compare, 1, duration, PgBenchLoadType.PGVECTOR_HALFVEC)
