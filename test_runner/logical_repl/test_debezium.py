"""
Test the logical replication in Neon with Debezium as a consumer
"""

from __future__ import annotations

import json
import os
import time

import psycopg2
import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import wait_until


class DebeziumAPI:
    """
    The class for Debezium API calls
    """

    def __init__(self):
        self.__host = "debezium" if ("CI" in os.environ) else "127.0.0.1"
        self.__base_url = f"http://{self.__host}:8083"
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
        return self.__request("DELETE", f"/{connector}")


@pytest.fixture(scope="function")
def debezium(remote_pg: RemotePostgres):
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
    dbz = DebeziumAPI()
    assert len(dbz.list_connectors()) == 0
    dbz_conn_name = "inventory-connector"
    resp = dbz.create_pg_connector(remote_pg, dbz_conn_name)
    log.debug("%s %s %s", resp.status_code, resp.ok, resp.text)
    assert resp.status_code == 201
    assert len(dbz.list_connectors()) == 1
    from kafka import KafkaConsumer

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
    assert msg, "Empty message"
    for val in msg.values():
        r = json.loads(val[-1].value)
        log.info(r["payload"])
        assert ts_ms < r["payload"]["ts_ms"], "Incorrect timestamp"
        for param, pname in ((before, "before"), (after, "after")):
            if param is not None:
                for k, v in param.items():
                    assert r["payload"][pname][k] == v, f"{pname} mismatches"


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
        lambda: get_kafka_msg(
            consumer,
            ts_ms,
            after={"first_name": "John", "last_name": "Dow", "email": "johndow@example.com"},
        ),
        timeout=60,
    )
    ts_ms = time.time() * 1000
    log.info("Insert 2 ts_ms: %s", ts_ms)
    cur.execute(
        "insert into inventory.customers (first_name, last_name, email) "
        "values ('Alex', 'Row','alexrow@example.com')"
    )
    conn.commit()
    wait_until(
        lambda: get_kafka_msg(
            consumer,
            ts_ms,
            after={"first_name": "Alex", "last_name": "Row", "email": "alexrow@example.com"},
        ),
        timeout=60,
    )
    ts_ms = time.time() * 1000
    log.info("Update ts_ms: %s", ts_ms)
    cur.execute("update inventory.customers set first_name = 'Alexander' where id = 2")
    conn.commit()
    wait_until(
        lambda: get_kafka_msg(
            consumer,
            ts_ms,
            after={"first_name": "Alexander"},
        ),
        timeout=60,
    )
    time.sleep(3)
    cur.execute("select 1")
