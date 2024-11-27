from __future__ import annotations

import pytest
from fixtures.log_helper import log
from fixtures.neon_cli import WalCraft
from fixtures.neon_fixtures import NeonEnvBuilder, PageserverWalReceiverProtocol

# Restart nodes with WAL end having specially crafted shape, like last record
# crossing segment boundary, to test decoding issues.


@pytest.mark.parametrize(
    "wal_type",
    [
        "simple",
        "last_wal_record_xlog_switch",
        "last_wal_record_xlog_switch_ends_on_page_boundary",
        "last_wal_record_crossing_segment",
        "wal_record_crossing_segment_followed_by_small_one",
    ],
)
@pytest.mark.parametrize(
    "wal_receiver_protocol",
    [PageserverWalReceiverProtocol.VANILLA, PageserverWalReceiverProtocol.INTERPRETED],
)
def test_crafted_wal_end(
    neon_env_builder: NeonEnvBuilder,
    wal_type: str,
    wal_receiver_protocol: PageserverWalReceiverProtocol,
):
    neon_env_builder.pageserver_wal_receiver_protocol = wal_receiver_protocol

    env = neon_env_builder.init_start()
    env.create_branch("test_crafted_wal_end")
    env.pageserver.allowed_errors.extend(
        [
            # seems like pageserver stop triggers these
            ".*initial size calculation failed.*Bad state (not active).*",
        ]
    )

    endpoint = env.endpoints.create("test_crafted_wal_end")
    wal_craft = WalCraft(extra_env=None, binpath=env.neon_binpath)
    endpoint.config(wal_craft.postgres_config())
    endpoint.start()
    res = endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE keys(key int primary key)",
            "INSERT INTO keys SELECT generate_series(1, 100)",
            "SELECT SUM(key) FROM keys",
        ]
    )
    assert res[-1][0] == (5050,)

    wal_craft.in_existing(wal_type, endpoint.connstr())

    log.info("Restarting all safekeepers and pageservers")
    env.pageserver.stop()
    env.safekeepers[0].stop()
    env.safekeepers[0].start()
    env.pageserver.start()

    log.info("Trying more queries")
    res = endpoint.safe_psql_many(
        queries=[
            "SELECT SUM(key) FROM keys",
            "INSERT INTO keys SELECT generate_series(101, 200)",
            "SELECT SUM(key) FROM keys",
        ]
    )
    assert res[0][0] == (5050,)
    assert res[-1][0] == (20100,)

    log.info("Restarting all safekeepers and pageservers (again)")
    env.pageserver.stop()
    env.safekeepers[0].stop()
    env.safekeepers[0].start()
    env.pageserver.start()

    log.info("Trying more queries (again)")
    res = endpoint.safe_psql_many(
        queries=[
            "SELECT SUM(key) FROM keys",
            "INSERT INTO keys SELECT generate_series(201, 300)",
            "SELECT SUM(key) FROM keys",
        ]
    )
    assert res[0][0] == (20100,)
    assert res[-1][0] == (45150,)
