import random
import threading
from enum import StrEnum
from time import sleep
from typing import Any

import pytest
from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC, wait_until
from prometheus_client.parser import text_string_to_metric_families as prom_parse_impl
from psycopg2.extensions import cursor as Cursor


class PrewarmMethod(StrEnum):
    POSTGRES = "postgres"
    COMPUTE_CTL = "compute-ctl"
    AUTOPREWARM = "autoprewarm"


PREWARM_LABEL = "compute_ctl_lfc_prewarms_total"
PREWARM_ERR_LABEL = "compute_ctl_lfc_prewarm_errors_total"
OFFLOAD_LABEL = "compute_ctl_lfc_offloads_total"
OFFLOAD_ERR_LABEL = "compute_ctl_lfc_offload_errors_total"
METHOD_VALUES = [e for e in PrewarmMethod]
METHOD_IDS = [e.value for e in PrewarmMethod]
AUTOOFFLOAD_INTERVAL_SECS = 2


def prom_parse(client: EndpointHttpClient) -> dict[str, float]:
    labels = PREWARM_LABEL, OFFLOAD_LABEL, PREWARM_ERR_LABEL, OFFLOAD_ERR_LABEL
    return {
        sample.name: int(sample.value)
        for family in prom_parse_impl(client.metrics())
        for sample in family.samples
        if sample.name in labels
    }


def offload_lfc(method: PrewarmMethod, client: EndpointHttpClient, cur: Cursor) -> Any:
    if method == PrewarmMethod.POSTGRES:
        cur.execute("select get_local_cache_state()")
        return cur.fetchall()[0][0]

    if method == PrewarmMethod.AUTOPREWARM:
        # With autoprewarm, we need to be sure LFC was offloaded after all writes
        # finish, so we sleep. Otherwise we'll have less prewarmed pages than we want
        sleep(AUTOOFFLOAD_INTERVAL_SECS)
        client.offload_lfc_wait()
        return

    if method == PrewarmMethod.COMPUTE_CTL:
        status = client.prewarm_lfc_status()
        assert status["status"] == "not_prewarmed"
        assert "error" not in status
        client.offload_lfc()
        assert client.prewarm_lfc_status()["status"] == "not_prewarmed"
        parsed = prom_parse(client)
        desired = {OFFLOAD_LABEL: 1, PREWARM_LABEL: 0, OFFLOAD_ERR_LABEL: 0, PREWARM_ERR_LABEL: 0}
        assert parsed == desired, f"{parsed=} != {desired=}"
        return

    raise AssertionError(f"{method} not in PrewarmMethod")


def prewarm_endpoint(
    method: PrewarmMethod, client: EndpointHttpClient, cur: Cursor, lfc_state: str | None
):
    if method == PrewarmMethod.AUTOPREWARM:
        client.prewarm_lfc_wait()
    elif method == PrewarmMethod.COMPUTE_CTL:
        client.prewarm_lfc()
    elif method == PrewarmMethod.POSTGRES:
        cur.execute("select prewarm_local_cache(%s)", (lfc_state,))


def check_prewarmed(
    method: PrewarmMethod, client: EndpointHttpClient, desired_status: dict[str, str | int]
):
    if method == PrewarmMethod.AUTOPREWARM:
        assert client.prewarm_lfc_status() == desired_status
        assert prom_parse(client)[PREWARM_LABEL] == 1
    elif method == PrewarmMethod.COMPUTE_CTL:
        assert client.prewarm_lfc_status() == desired_status
        desired = {OFFLOAD_LABEL: 0, PREWARM_LABEL: 1, PREWARM_ERR_LABEL: 0, OFFLOAD_ERR_LABEL: 0}
        assert prom_parse(client) == desired


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("method", METHOD_VALUES, ids=METHOD_IDS)
def test_lfc_prewarm(neon_simple_env: NeonEnv, method: PrewarmMethod):
    """
    Test we can offload endpoint's LFC cache to endpoint storage.
    Test we can prewarm endpoint with LFC cache loaded from endpoint storage.
    """
    env = neon_simple_env
    n_records = 1000000
    cfg = [
        "autovacuum = off",
        "shared_buffers=1MB",
        "neon.max_file_cache_size=1GB",
        "neon.file_cache_size_limit=1GB",
        "neon.file_cache_prewarm_limit=1000",
    ]

    if method == PrewarmMethod.AUTOPREWARM:
        endpoint = env.endpoints.create_start(
            branch_name="main",
            config_lines=cfg,
            autoprewarm=True,
            offload_lfc_interval_seconds=AUTOOFFLOAD_INTERVAL_SECS,
        )
    else:
        endpoint = env.endpoints.create_start(branch_name="main", config_lines=cfg)

    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create extension neon")
    pg_cur.execute("create database lfc")

    lfc_conn = endpoint.connect(dbname="lfc")
    lfc_cur = lfc_conn.cursor()
    log.info(f"Inserting {n_records} rows")
    lfc_cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")
    lfc_cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")
    log.info(f"Inserted {n_records} rows")

    client = endpoint.http_client()
    lfc_state = offload_lfc(method, client, pg_cur)

    endpoint.stop()
    if method == PrewarmMethod.AUTOPREWARM:
        endpoint.start(autoprewarm=True, offload_lfc_interval_seconds=AUTOOFFLOAD_INTERVAL_SECS)
    else:
        endpoint.start()

    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()

    lfc_conn = endpoint.connect(dbname="lfc")
    lfc_cur = lfc_conn.cursor()
    prewarm_endpoint(method, client, pg_cur, lfc_state)

    pg_cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_used_pages'")
    lfc_used_pages = pg_cur.fetchall()[0][0]
    log.info(f"Used LFC size: {lfc_used_pages}")
    pg_cur.execute("select * from get_prewarm_info()")
    total, prewarmed, skipped, _ = pg_cur.fetchall()[0]
    log.info(f"Prewarm info: {total=} {prewarmed=} {skipped=}")
    progress = (prewarmed + skipped) * 100 // total
    log.info(f"Prewarm progress: {progress}%")
    assert lfc_used_pages > 10000
    assert total > 0
    assert prewarmed > 0
    assert total == prewarmed + skipped

    lfc_cur.execute("select sum(pk) from t")
    assert lfc_cur.fetchall()[0][0] == n_records * (n_records + 1) / 2

    desired = {"status": "completed", "total": total, "prewarmed": prewarmed, "skipped": skipped}
    check_prewarmed(method, client, desired)


# autoprewarm isn't needed as we prewarm manually
WORKLOAD_VALUES = METHOD_VALUES[:-1]
WORKLOAD_IDS = METHOD_IDS[:-1]


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("method", WORKLOAD_VALUES, ids=WORKLOAD_IDS)
def test_lfc_prewarm_under_workload(neon_simple_env: NeonEnv, method: PrewarmMethod):
    """
    Test continiously prewarming endpoint when there is a write-heavy workload going in parallel
    """
    env = neon_simple_env
    n_records = 10000
    n_threads = 4
    cfg = [
        "shared_buffers=1MB",
        "neon.max_file_cache_size=1GB",
        "neon.file_cache_size_limit=1GB",
        "neon.file_cache_prewarm_limit=1000000",
    ]
    endpoint = env.endpoints.create_start(branch_name="main", config_lines=cfg)

    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create extension neon")
    pg_cur.execute("CREATE DATABASE lfc")

    lfc_conn = endpoint.connect(dbname="lfc")
    lfc_cur = lfc_conn.cursor()
    lfc_cur.execute(
        "create table accounts(id integer primary key, balance bigint default 0, payload text default repeat('?', 1000)) with (fillfactor=10)"
    )
    log.info(f"Inserting {n_records} rows")
    lfc_cur.execute(f"insert into accounts(id) values (generate_series(1,{n_records}))")
    log.info(f"Inserted {n_records} rows")

    http_client = endpoint.http_client()
    lfc_state = offload_lfc(method, http_client, pg_cur)
    running = True
    n_prewarms = 0

    def workload():
        lfc_conn = endpoint.connect(dbname="lfc")
        lfc_cur = lfc_conn.cursor()
        n_transfers = 0
        while running:
            src = random.randint(1, n_records)
            dst = random.randint(1, n_records)
            lfc_cur.execute(f"update accounts set balance=balance-100 where id={src}")
            lfc_cur.execute(f"update accounts set balance=balance+100 where id={dst}")
            n_transfers += 1
        log.info(f"Number of transfers: {n_transfers}")

    def prewarm():
        pg_conn = endpoint.connect()
        pg_cur = pg_conn.cursor()
        while running:
            pg_cur.execute("alter system set neon.file_cache_size_limit='1MB'")
            pg_cur.execute("select pg_reload_conf()")
            pg_cur.execute("alter system set neon.file_cache_size_limit='1GB'")
            pg_cur.execute("select pg_reload_conf()")
            prewarm_endpoint(method, http_client, pg_cur, lfc_state)
            nonlocal n_prewarms
            n_prewarms += 1
        log.info(f"Number of prewarms: {n_prewarms}")

    workload_threads = []
    for _ in range(n_threads):
        t = threading.Thread(target=workload)
        workload_threads.append(t)
        t.start()

    prewarm_thread = threading.Thread(target=prewarm)
    prewarm_thread.start()

    def prewarmed():
        assert n_prewarms > 3

    wait_until(prewarmed, timeout=40)  # debug builds don't finish in 20s

    running = False
    for t in workload_threads:
        t.join()
    prewarm_thread.join()

    lfc_cur.execute("select sum(balance) from accounts")
    total_balance = lfc_cur.fetchall()[0][0]
    assert total_balance == 0

    if method == PrewarmMethod.POSTGRES:
        return
    desired = {
        OFFLOAD_LABEL: 1,
        PREWARM_LABEL: n_prewarms,
        OFFLOAD_ERR_LABEL: 0,
        PREWARM_ERR_LABEL: 0,
    }
    assert prom_parse(http_client) == desired
