from __future__ import annotations

import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PageserverWalReceiverProtocol,
    check_restored_datadir_content,
)


# Test subtransactions
#
# The pg_subxact SLRU is not preserved on restarts, and doesn't need to be
# maintained in the pageserver, so subtransactions are not very exciting for
# Neon. They are included in the commit record though and updated in the
# CLOG.
@pytest.mark.parametrize(
    "wal_receiver_protocol",
    [PageserverWalReceiverProtocol.VANILLA, PageserverWalReceiverProtocol.INTERPRETED],
)
def test_subxacts(neon_env_builder: NeonEnvBuilder, test_output_dir, wal_receiver_protocol):
    neon_env_builder.pageserver_wal_receiver_protocol = wal_receiver_protocol

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE TABLE t1(i int, j int);")

    cur.execute("select pg_switch_wal();")

    # Issue 100 transactions, with 1000 subtransactions in each.
    for i in range(100):
        cur.execute("begin")
        for j in range(1000):
            cur.execute(f"savepoint sp{j}")
            cur.execute(f"insert into t1 values ({i}, {j})")
        cur.execute("commit")

    check_restored_datadir_content(test_output_dir, env, endpoint)
