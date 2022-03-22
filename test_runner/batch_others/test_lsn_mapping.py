from contextlib import closing
from uuid import UUID
import psycopg2.extras
import psycopg2.errors
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, Postgres
from fixtures.log_helper import log
import time


def test_lsn_mapping(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    # Branch at the point where only 100 rows were inserted
    new_timeline_id = env.zenith_cli.create_branch('test_timeline_size', 'empty')

    pgmain = env.postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with closing(env.pageserver.connect()) as ps_conn:
            with conn.cursor() as cur:
                with ps_conn.cursor() as ps_cur:
                    # Create table, and insert the first 100 rows
                    cur.execute("CREATE TABLE foo (x integer)")
                    cur.execute("INSERT INTO foo values (1)")
                    cur.execute("SELECT txid_current()")
                    xid = cur.fetchone()[0]
                    cur.execute("SELECT pg_current_wal_lsn()")
                    lsn = int(cur.fetchone()[0][2:], 16)
                    time.sleep(1)
                    cur.execute("INSERT INTO foo values (2)")

                    ps_cur.execute(
                        f"get_lsn_by_xid {env.initial_tenant.hex} {new_timeline_id.hex} {xid}")
                    assert ps_cur.fetchone() == (lsn, )

                    ps_cur.execute(
                        f"get_lsn_by_timestamp {env.initial_tenant.hex} {new_timeline_id.hex} 1sec")
                    assert ps_cur.fetchone() == (lsn, )
