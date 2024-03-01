from pathlib import Path

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar


def test_lfc_working_set_approximation(neon_simple_env: NeonEnv):
    env = neon_simple_env

    cache_dir = Path(env.repo_dir) / "file_cache"
    cache_dir.mkdir(exist_ok=True)

    branchname = "test_approximate_working_set_size"
    env.neon_cli.create_branch(branchname, "empty")
    log.info(f"Creating endopint with 1MB shared_buffers and 64 MB LFC for branch {branchname}")
    endpoint = env.endpoints.create_start(
        branchname,
        config_lines=[
            "shared_buffers='1MB'",
            f"neon.file_cache_path='{cache_dir}/file.cache'",
            "neon.max_file_cache_size='128MB'",
            "neon.file_cache_size_limit='64MB'",
        ],
    )

    cur = endpoint.connect().cursor()
    cur.execute("create extension if not exists neon")

    log.info(f"preparing some data in {endpoint.connstr()}")

    ddl = """
CREATE TABLE pgbench_accounts (
    aid bigint NOT NULL,
    bid integer,
    abalance integer,
    filler character(84),
    -- more web-app like columns
    text_column_plain TEXT  DEFAULT repeat('NeonIsCool', 5),
    jsonb_column_extended JSONB  DEFAULT ('{ "tell everyone": [' || repeat('{"Neon": "IsCool"},',9) || ' {"Neon": "IsCool"}]}')::jsonb
)
WITH (fillfactor='100');
"""

    cur.execute(ddl)
    # prepare index access below
    cur.execute(
        "ALTER TABLE ONLY pgbench_accounts ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid)"
    )
    cur.execute(
        "insert into pgbench_accounts(aid,bid,abalance,filler) select aid, (aid - 1) / 100000 + 1, 0, '' from generate_series(1, 100000) as aid;"
    )
    # ensure correct query plans and stats
    cur.execute("vacuum ANALYZE pgbench_accounts")
    # determine table size - working set should approximate table size after sequential scan
    pages = query_scalar(cur, "SELECT relpages FROM pg_class WHERE relname = 'pgbench_accounts'")
    log.info(f"pgbench_accounts has {pages} pages, resetting working set to zero")
    cur.execute("select approximate_working_set_size(true)")
    cur.execute(
        'SELECT count(*) FROM pgbench_accounts WHERE abalance > 0 or jsonb_column_extended @> \'{"tell everyone": [{"Neon": "IsCool"}]}\'::jsonb'
    )
    # verify working set size after sequential scan matches table size and reset working set for next test
    blocks = query_scalar(cur, "select approximate_working_set_size(true)")
    log.info(f"working set size after sequential scan on pgbench_accounts {blocks}")
    assert pages * 0.8 < blocks < pages * 1.2
    # run a few point queries with index lookup
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid =   4242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid =  54242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid = 104242")
    cur.execute("SELECT abalance FROM pgbench_accounts WHERE aid = 204242")
    # verify working set size after some index access of a few select pages only
    blocks = query_scalar(cur, "select approximate_working_set_size(true)")
    log.info(f"working set size after some index access of a few select pages only {blocks}")
    assert blocks < 10
    cur.execute("drop table pgbench_accounts")
