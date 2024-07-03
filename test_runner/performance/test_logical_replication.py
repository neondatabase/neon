from __future__ import annotations

import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import AuxFileStore, logical_replication_sync

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv, PgBin


@pytest.mark.parametrize("pageserver_aux_file_policy", [AuxFileStore.V2])
@pytest.mark.timeout(1000)
def test_logical_replication(neon_simple_env: NeonEnv, pg_bin: PgBin, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start("test_logical_replication")

    log.info("postgres is running on 'test_logical_replication' branch")
    pg_bin.run_capture(["pgbench", "-i", "-s10", endpoint.connstr()])

    endpoint.safe_psql("create publication pub1 for table pgbench_accounts, pgbench_history")

    # now start subscriber
    vanilla_pg.start()
    pg_bin.run_capture(["pgbench", "-i", "-s10", vanilla_pg.connstr()])

    vanilla_pg.safe_psql("truncate table pgbench_accounts")
    vanilla_pg.safe_psql("truncate table pgbench_history")

    connstr = endpoint.connstr().replace("'", "''")
    print(f"connstr='{connstr}'")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)

    pg_bin.run_capture(["pgbench", "-c10", "-T100", "-Mprepared", endpoint.connstr()])

    # Wait logical replication to sync
    start = time.time()
    logical_replication_sync(vanilla_pg, endpoint)
    log.info(f"Sync with master took {time.time() - start} seconds")

    sum_master = endpoint.safe_psql("select sum(abalance) from pgbench_accounts")[0][0]
    sum_replica = vanilla_pg.safe_psql("select sum(abalance) from pgbench_accounts")[0][0]
    assert sum_master == sum_replica
