import os
from pathlib import Path

import pytest
from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.compare_fixtures import RemoteCompare
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.utils import shared_buffers_for_max_cu


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
@pytest.mark.remote_cluster
def test_perf_many_relations(remote_compare: RemoteCompare, num_relations: int):
    """
    Test creating many relations in a single database.
    We use partitioned tables with child tables, indexes and constraints to have a realistic schema.
    Also we include some common data types like text, uuid, timestamp, JSONB, etc.

    see many_relations/create_many_relations.sql
    """
    env = remote_compare

    # prepare some base tables and the plpgsql procedures that we use to create the tables
    sql_file = Path(__file__).parent / "many_relations" / "create_many_relations.sql"
    env.pg_bin.run_capture(["psql", env.pg.connstr(), "-f", str(sql_file)])

    num_parent_tables = num_relations // 500 + 1
    log.info(f"Creating {num_relations} relations in {num_parent_tables} parent tables")

    log.info(f"Creating {num_parent_tables} parent tables")
    sql = f"CALL create_partitioned_tables('operations_scale_{num_relations}', {num_parent_tables})"
    log.info(sql)
    env.pg_bin.run_capture(["psql", env.pg.connstr(), "-c", sql])

    current_table = 0
    num_relations_remaining = num_relations

    # now run and measure the actual relation creation
    while num_relations_remaining > 0:
        current_table += 1
        parent_table_name = f"operations_scale_{num_relations}_{current_table}"
        if num_relations_remaining > 500:
            num_relations_to_create = 500
        else:
            num_relations_to_create = num_relations_remaining
        num_relations_remaining -= num_relations_to_create
        log.info(
            f"Creating {num_relations_to_create} child tables in partitioned parent table '{parent_table_name}'"
        )
        sql = f"CALL create_operations_partitions( '{parent_table_name}', '2000-01-01', ('2000-01-01'::DATE + INTERVAL '1 day' * {num_relations_to_create})::DATE)"
        log.info(sql)
        with env.zenbenchmark.record_duration(
            f"CREATE_TABLE/{current_table}/{num_relations_to_create}"
        ):
            env.pg_bin.run_capture(
                ["psql", env.pg.connstr(options="-cstatement_timeout=1000s "), "-c", sql]
            )


@pytest.mark.parametrize(
    "reldir,num_relations",
    [("v1", 10000), ("v1v2", 10000), ("v2", 10000), ("v2", 100000)],
    ids=["v1-small", "v1v2-small", "v2-small", "v2-large"],
)
def test_perf_simple_many_relations_reldir(
    neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker, reldir: str, num_relations: int
):
    """
    Test creating many relations in a single database.
    """
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "rel_size_v2_enabled": reldir != "v1",
            "rel_size_v1_access_disabled": reldir == "v2",
        }
    )
    ep = env.endpoints.create_start(
        "main",
        config_lines=[
            # use shared_buffers size like in production for 8 CU compute
            f"shared_buffers={shared_buffers_for_max_cu(8.0)}",
            "max_locks_per_transaction=16384",
        ],
    )

    ep.safe_psql("CREATE TABLE IF NOT EXISTS initial_table (v1 int)")
    wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    if reldir == "v1":
        assert (
            env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
                "rel_size_migration"
            ]
            == "legacy"
        )
    elif reldir == "v1v2":
        assert (
            env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
                "rel_size_migration"
            ]
            == "migrating"
        )
    elif reldir == "v2":
        assert (
            env.pageserver.http_client().timeline_detail(env.initial_tenant, env.initial_timeline)[
                "rel_size_migration"
            ]
            == "migrated"
        )
    else:
        raise AssertionError(f"Invalid reldir config: {reldir}")

    n = num_relations
    step = 5000
    # Create many relations
    log.info(f"Creating {n} relations...")
    begin = 0
    with zenbenchmark.record_duration("create_first_relation"):
        ep.safe_psql("CREATE TABLE IF NOT EXISTS table_begin (id SERIAL PRIMARY KEY, data TEXT)")
    with zenbenchmark.record_duration("create_many_relations"):
        while True:
            end = begin + step
            ep.safe_psql_many(
                [
                    "BEGIN",
                    f"""DO $$
                DECLARE
                    i INT;
                    table_name TEXT;
                BEGIN
                    FOR i IN {begin}..{end} LOOP
                        table_name := 'table_' || i;
                        EXECUTE 'CREATE TABLE IF NOT EXISTS ' || table_name || ' (id SERIAL PRIMARY KEY, data TEXT)';
                    END LOOP;
                END $$;
                """,
                    "COMMIT",
                ]
            )
            begin = end
            if begin >= n:
                break
    with zenbenchmark.record_duration("create_last_relation"):
        ep.safe_psql(f"CREATE TABLE IF NOT EXISTS table_{begin} (id SERIAL PRIMARY KEY, data TEXT)")
