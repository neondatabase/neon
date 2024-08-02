"""
Test the logical replication in Neon with the different consumers
"""

import hashlib
import json
import time

import clickhouse_connect
import psycopg2
import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import wait_until
from kafka import KafkaConsumer


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


class DebeziumAPI:
    """
    The class for Debeziym API calls
    """

    def __init__(self, base_url: str):
        self.__base_url = base_url
        self.__connectors_url = f"{self.__base_url}/connectors"

    def __request(self, method, addurl="", **kwargs):
        return requests.request(
            method,
            self.__connectors_url + addurl,
            headers={"Accept": "application/json", "Content-type": "application/json"},
            timeout=60,
            **kwargs,
        )

    def create_pg_connector(self, remote_pg: RemotePostgres, dbz_conn_name: str):
        """
        Create a Postgres connector in debezium
        """
        conn_options = remote_pg.conn_options()
        payload = {
            "name": dbz_conn_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.hostname": conn_options["host"],
                "database.port": "5432",
                "database.user": conn_options["user"],
                "database.password": conn_options["password"],
                "database.dbname": conn_options["dbname"],
                "plugin.name": "pgoutput",
                "topic.prefix": "dbserver1",
                "schema.include.list": "inventory",
            },
        }
        return self.__request("POST", json=payload)

    def list_connectors(self):
        """
        Returns a list of all connectors existent in Debezium.
        """
        resp = self.__request("GET")
        assert resp.ok
        return json.loads(resp.text)

    def del_connector(self, connector):
        """
        Deletes the specified connector
        """
        return self.__request("DELETE", "/" + connector)


@pytest.mark.remote_cluster
def test_clickhouse(remote_pg: RemotePostgres):
    """
    Test the logical replication having ClickHouse as a client
    """
    conn_options = remote_pg.conn_options()
    conn = psycopg2.connect(remote_pg.connstr())
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS table1")
    cur.execute("CREATE TABLE table1 (id integer primary key, column1 varchar(10));")
    cur.execute("INSERT INTO table1 (id, column1) VALUES (1, 'abc'), (2, 'def');")
    conn.commit()
    client = clickhouse_connect.get_client(host="clickhouse")
    client.command("SET allow_experimental_database_materialized_postgresql=1")
    client.command(
        "CREATE DATABASE db1_postgres ENGINE = "
        f"MaterializedPostgreSQL('{conn_options['host']}', "
        f"'{conn_options['dbname']}', "
        f"'{conn_options['user']}', '{conn_options['password']}') "
        "SETTINGS materialized_postgresql_tables_list = 'table1';"
    )
    wait_until(
        120,
        0.5,
        lambda: query_clickhouse(
            client,
            "select * from db1_postgres.table1 order by 1",
            "ee600d8f7cd05bd0b169fa81f44300a9dd10085a",
        ),
    )
    cur.execute("INSERT INTO table1 (id, column1) VALUES (3, 'ghi'), (4, 'jkl');")
    conn.commit()
    wait_until(
        120,
        0.5,
        lambda: query_clickhouse(
            client,
            "select * from db1_postgres.table1 order by 1",
            "9eba2daaf7e4d7d27ac849525f68b562ab53947d",
        ),
    )
    log.debug("Sleeping before final checking if Neon is still alive")
    time.sleep(3)
    cur.execute("SELECT 1")


@pytest.fixture(scope="function")
def debezium(remote_pg):
    """
    Prepare the Debezium API handler, connection
    """
    conn = psycopg2.connect(remote_pg.connstr())
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS inventory CASCADE")
    cur.execute("CREATE SCHEMA inventory")
    cur.execute(
        "CREATE TABLE inventory.customers ("
        "id SERIAL NOT NULL PRIMARY KEY,"
        "first_name character varying(255) NOT NULL,"
        "last_name character varying(255) NOT NULL,"
        "email character varying(255) NOT NULL)"
    )
    conn.commit()
    dbz = DebeziumAPI("http://debezium:8083")
    assert len(dbz.list_connectors()) == 0
    dbz_conn_name = "inventory-connector"
    resp = dbz.create_pg_connector(remote_pg, dbz_conn_name)
    log.debug("%s %s %s", resp.status_code, resp.ok, resp.text)
    assert resp.status_code == 201
    assert len(dbz.list_connectors()) == 1
    consumer = KafkaConsumer(
        "dbserver1.inventory.customers",
        bootstrap_servers=["kafka:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    yield conn, consumer
    resp = dbz.del_connector(dbz_conn_name)
    assert resp.status_code == 204


def get_kafka_msg(consumer, ts_ms, before=None, after=None) -> None:
    """
    Gets the message from Kafka and checks its validity
    Arguments:
        consumer: the consumer object
        ts_ms:    timestamp in milliseconds of the change of db, the corresponding message must have
                  the later timestamp
        before:   a dictionary, if not None, the before field from the kafka message must
                  have the same values for the same keys
        after:    a dictionary, if not None, the after field from the kafka message must
                  have the same values for the same keys
    """
    msg = consumer.poll()
    if not msg:
        raise ValueError("Empty message")
    for val in msg.values():
        r = json.loads(val[-1].value)
        log.info(r["payload"])
        if ts_ms >= r["payload"]["ts_ms"]:
            log.info("incorrect ts")
            raise ValueError("Incorrect timestamp")
        for param, pname in ((before, "before"), (after, "after")):
            if param is not None:
                for k, v in param.items():
                    if r["payload"][pname][k] != v:
                        log.info("%s mismatch", pname)
                        raise ValueError(pname + " mismatches")


# pylint: disable=redefined-outer-name
@pytest.mark.remote_cluster
def test_debezium(debezium):
    """
    Test the logical replication having Debezium as a subscriber
    """
    conn, consumer = debezium
    cur = conn.cursor()
    ts_ms = time.time() * 1000
    log.info("Insert 1 ts_ms: %s", ts_ms)
    cur.execute(
        "insert into inventory.customers (first_name, last_name, email) "
        "values ('John', 'Dow','johndow@example.com')"
    )
    conn.commit()
    wait_until(
        100,
        0.5,
        lambda: get_kafka_msg(
            consumer,
            ts_ms,
            after={"first_name": "John", "last_name": "Dow", "email": "johndow@example.com"},
        ),
    )
    ts_ms = time.time() * 1000
    log.info("Insert 2 ts_ms: %s", ts_ms)
    cur.execute(
        "insert into inventory.customers (first_name, last_name, email) "
        "values ('Alex', 'Row','alexrow@example.com')"
    )
    conn.commit()
    wait_until(
        100,
        0.5,
        lambda: get_kafka_msg(
            consumer,
            ts_ms,
            after={"first_name": "Alex", "last_name": "Row", "email": "alexrow@example.com"},
        ),
    )
    log.info("Update ts_ms: %s", ts_ms)
    ts_ms = time.time() * 1000
    cur.execute("update inventory.customers set first_name = 'Alexander' where id = 2")
    conn.commit()
    wait_until(100, 0.5, lambda: get_kafka_msg(consumer, ts_ms, after={"first_name": "Alexander"}))
    time.sleep(3)
    cur.execute("select 1")
