import asyncio
import json
import subprocess
import time
import urllib.parse
from typing import Any, List, Optional, Tuple

import psycopg2
import pytest
import requests
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres

GET_CONNECTION_PID_QUERY = "SELECT pid FROM pg_stat_activity WHERE state = 'active'"


def test_sql_over_http(static_auth_broker: NeonProxy):
    static_auth_broker.safe_psql("create role http with login password 'http' superuser")

    def q(sql: str, params: Optional[List[Any]] = None) -> Any:
        params = params or []
        connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={"Content-Type": "application/sql", "Neon-Connection-String": connstr},
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200, response.text
        return response.json()

    rows = q("select 42 as answer")["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1 as answer", [42])["rows"]
    assert rows == [{"answer": "42"}]

    rows = q("select $1 * 1 as answer", [42])["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1::int[] as answer", [[1, 2, 3]])["rows"]
    assert rows == [{"answer": [1, 2, 3]}]

    rows = q("select $1::json->'a' as answer", [{"a": {"b": 42}}])["rows"]
    assert rows == [{"answer": {"b": 42}}]

    rows = q("select $1::jsonb[] as answer", [[{}]])["rows"]
    assert rows == [{"answer": [{}]}]

    rows = q("select $1::jsonb[] as answer", [[{"foo": 1}, {"bar": 2}]])["rows"]
    assert rows == [{"answer": [{"foo": 1}, {"bar": 2}]}]

    rows = q("select * from pg_class limit 1")["rows"]
    assert len(rows) == 1

    res = q("create table t(id serial primary key, val int)")
    assert res["command"] == "CREATE"
    assert res["rowCount"] is None

    res = q("insert into t(val) values (10), (20), (30) returning id")
    assert res["command"] == "INSERT"
    assert res["rowCount"] == 3
    assert res["rows"] == [{"id": 1}, {"id": 2}, {"id": 3}]

    res = q("select * from t")
    assert res["command"] == "SELECT"
    assert res["rowCount"] == 3

    res = q("drop table t")
    assert res["command"] == "DROP"
    assert res["rowCount"] is None

