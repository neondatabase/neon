"""
Test the logical replication in Neon with the different consumers
"""
import hashlib
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
import psycopg2
import pytest
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError

from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import subprocess_capture
from fixtures.log_helper import log


def timeout_query_clickhouse(client, query: str, digest: str,
                             timeout: timedelta = timedelta(seconds=30), interval: float = 0.5):
    """
    Repeatedly run the query on the client
    return answer if successful, raise an exception otherwise
    """
    start = datetime.now()
    while datetime.now() - start <= timeout:
        hash_res = res = None
        try:
            res = client.query(query)
        except DatabaseError as dbe:
            log.debug('Query: %s', query)
            log.debug(dbe)
        else:
            m = hashlib.sha1()
            m.update(repr(tuple(res.result_rows)).encode())
            hash_res = m.hexdigest()
        if res:
            log.debug(res.result_rows)
        if hash_res:
            log.debug('Hash: %s', hash_res)
        if hash_res == digest:
            return res
        time.sleep(interval)
    raise TimeoutError


def clickhouse_cleanup(test_output_dir: Path):
    """
    Remove the clickhouse container
    """
    log.info("Stopping clickhouse")
    cmd = ["docker", "compose", "down"]
    subprocess_capture(test_output_dir, cmd, check=True, capture_stdout=True)


@pytest.mark.remote_cluster
def test_clickhouse(test_output_dir: Path, remote_pg: RemotePostgres):
    """
    Test the logical replication having ClickHouse as a client
    """
    os.chdir(os.path.join(Path(__file__).parent, 'clickhouse'))
    conn_options = remote_pg.conn_options()
    cmd = ["docker", "compose", "up", "-d", "--quiet-pull", "--no-color", "--wait"]
    subprocess_capture(test_output_dir, cmd, check=True, capture_stdout=True)
    try:
        conn = None
        for _ in range(5):
            try:
                conn = psycopg2.connect(remote_pg.connstr())
            except psycopg2.OperationalError as perr:
                log.debug(perr)
                time.sleep(1)
            else:
                break
            raise TimeoutError
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS table1")
        cur.execute("CREATE TABLE table1 (id integer primary key, column1 varchar(10));")
        cur.execute("INSERT INTO table1 (id, column1) VALUES (1, 'abc'), (2, 'def');")
        conn.commit()
        client = clickhouse_connect.get_client(host='localhost')
        client.command("SET allow_experimental_database_materialized_postgresql=1")
        client.command("CREATE DATABASE db1_postgres ENGINE = "
                       f"MaterializedPostgreSQL('{conn_options['host']}', "
                       f"'{conn_options['dbname']}', "
                       f"'{conn_options['user']}', '{conn_options['password']}') "
                       "SETTINGS materialized_postgresql_tables_list = 'table1';")
        res = timeout_query_clickhouse(client,
                                       "select * from db1_postgres.table1 order by 1",
                                       'ee600d8f7cd05bd0b169fa81f44300a9dd10085a')
        log.debug(res.result_rows)
        cur.execute("INSERT INTO table1 (id, column1) VALUES (3, 'ghi'), (4, 'jkl');")
        conn.commit()
        res = timeout_query_clickhouse(client,
                                       "select * from db1_postgres.table1 order by 1",
                                       '9eba2daaf7e4d7d27ac849525f68b562ab53947d')
        log.debug(res.result_rows)
        log.debug('Sleeping before final checking if Neon is still alive')
        time.sleep(3)
        cur.execute('SELECT 1')
        clickhouse_cleanup(test_output_dir)
    except Exception as e:
        clickhouse_cleanup(test_output_dir)
        raise e
