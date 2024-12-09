"""
Test the logical replication in Neon with ClickHouse as a consumer
"""

from __future__ import annotations

import hashlib
import os
import time

import clickhouse_connect
import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import wait_until


def query_clickhouse(
    client,
    query: str,
    digest: str,
) -> None:
    """
    Run the query on the client
    return answer if successful, raise an exception otherwise
    """
    log.debug("Query: %s", query)
    res = client.query(query)
    log.debug(res.result_rows)
    m = hashlib.sha1()
    m.update(repr(tuple(res.result_rows)).encode())
    hash_res = m.hexdigest()
    log.debug("Hash: %s", hash_res)
    if hash_res == digest:
        return
    raise ValueError("Hash mismatch")


@pytest.mark.remote_cluster
def test_clickhouse(remote_pg: RemotePostgres):
    """
    Test the logical replication having ClickHouse as a client
    """
    clickhouse_host = "clickhouse" if ("CI" in os.environ) else "127.0.0.1"
    conn_options = remote_pg.conn_options()
    conn = psycopg2.connect(remote_pg.connstr())
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS table1")
    cur.execute("CREATE TABLE table1 (id integer primary key, column1 varchar(10));")
    cur.execute("INSERT INTO table1 (id, column1) VALUES (1, 'abc'), (2, 'def');")
    conn.commit()
    client = clickhouse_connect.get_client(host=clickhouse_host)
    client.command("SET allow_experimental_database_materialized_postgresql=1")
    client.command(
        "CREATE DATABASE db1_postgres ENGINE = "
        f"MaterializedPostgreSQL('{conn_options['host']}', "
        f"'{conn_options['dbname']}', "
        f"'{conn_options['user']}', '{conn_options['password']}') "
        "SETTINGS materialized_postgresql_tables_list = 'table1';"
    )
    wait_until(
        lambda: query_clickhouse(
            client,
            "select * from db1_postgres.table1 order by 1",
            "ee600d8f7cd05bd0b169fa81f44300a9dd10085a",
        ),
        timeout=60,
    )
    cur.execute("INSERT INTO table1 (id, column1) VALUES (3, 'ghi'), (4, 'jkl');")
    conn.commit()
    wait_until(
        lambda: query_clickhouse(
            client,
            "select * from db1_postgres.table1 order by 1",
            "9eba2daaf7e4d7d27ac849525f68b562ab53947d",
        ),
        timeout=60,
    )
    log.debug("Sleeping before final checking if Neon is still alive")
    time.sleep(3)
    cur.execute("SELECT 1")
