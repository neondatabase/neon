import random
import threading
import time
from enum import Enum

import pytest
from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC
from prometheus_client.parser import text_string_to_metric_families as prom_parse_impl


class LfcQueryMethod(Enum):
    COMPUTE_CTL = False
    POSTGRES = True


PREWARM_LABEL = "compute_ctl_lfc_prewarm_requests_total"
OFFLOAD_LABEL = "compute_ctl_lfc_offload_requests_total"
QUERY_OPTIONS = LfcQueryMethod.POSTGRES, LfcQueryMethod.COMPUTE_CTL


def check_pinned_entries(cur):
    # some LFC buffer can be temporary locked by autovacuum or background writer
    for _ in range(10):
        cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_chunks_pinned'")
        n_pinned = cur.fetchall()[0][0]
        if n_pinned == 0:
            break
        time.sleep(1)
    assert n_pinned == 0


def prom_parse(client: EndpointHttpClient) -> dict[str, float]:
    return {
        sample.name: sample.value
        for family in prom_parse_impl(client.metrics())
        for sample in family.samples
        if sample.name in (PREWARM_LABEL, OFFLOAD_LABEL)
    }


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("query", QUERY_OPTIONS, ids=["pg-only", "compute-ctl"])
def test_lfc_prewarm(neon_simple_env: NeonEnv, query: LfcQueryMethod):
    env = neon_simple_env
    n_records = 1000000
    endpoint = env.endpoints.create_start(
        branch_name="main",
        config_lines=[
            "autovacuum = off",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_limit=1000",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon version '1.6'")
    cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")
    cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")

    http_client = endpoint.http_client()
    if query == LfcQueryMethod.COMPUTE_CTL:
        status = http_client.prewarm_lfc_status()
        assert status["status"] == "not_prewarmed"
        assert "error" not in status
        http_client.offload_lfc()
        assert http_client.prewarm_lfc_status()["status"] == "not_prewarmed"
        assert prom_parse(http_client) == {OFFLOAD_LABEL: 1, PREWARM_LABEL: 0}
    else:
        cur.execute("select get_local_cache_state()")
        lfc_state = cur.fetchall()[0][0]

    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()
    # wait until compute_ctl complete downgrade of extension to default version
    time.sleep(1)
    cur.execute("alter extension neon update to '1.6'")

    if query == LfcQueryMethod.COMPUTE_CTL:
        http_client.prewarm_lfc()
    else:
        cur.execute("select prewarm_local_cache(%s)", (lfc_state,))

    cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_used_pages'")
    lfc_used_pages = cur.fetchall()[0][0]
    log.info(f"Used LFC size: {lfc_used_pages}")
    cur.execute("select * from get_prewarm_info()")
    prewarm_info = cur.fetchall()[0]
    log.info(f"Prewarm info: {prewarm_info}")
    total, prewarmed, skipped, _ = prewarm_info
    progress = (prewarmed + skipped) * 100 // total
    log.info(f"Prewarm progress: {progress}%")

    assert lfc_used_pages > 10000
    assert (
        prewarm_info[0] > 0
        and prewarm_info[1] > 0
        and prewarm_info[0] == prewarm_info[1] + prewarm_info[2]
    )

    cur.execute("select sum(pk) from t")
    assert cur.fetchall()[0][0] == n_records * (n_records + 1) / 2

    check_pinned_entries(cur)
    if query == LfcQueryMethod.COMPUTE_CTL:
        desired = {
            "status": "completed",
            "total": total,
            "prewarmed": prewarmed,
            "skipped": skipped,
        }
        assert http_client.prewarm_lfc_status() == desired
        assert prom_parse(http_client) == {OFFLOAD_LABEL: 0, PREWARM_LABEL: 1}


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("query", QUERY_OPTIONS, ids=["pg-only", "compute-ctl"])
def test_lfc_prewarm_under_workload(neon_simple_env: NeonEnv, query: LfcQueryMethod):
    """
    with_compute_ctl: Test compute_ctl's methods instead of querying Postgres directly
    """
    env = neon_simple_env
    n_records = 10000
    n_threads = 4
    endpoint = env.endpoints.create_start(
        branch_name="main",
        config_lines=[
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_limit=1000000",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon version '1.6'")
    cur.execute(
        "create table accounts(id integer primary key, balance bigint default 0, payload text default repeat('?', 1000)) with (fillfactor=10)"
    )
    cur.execute(f"insert into accounts(id) values (generate_series(1,{n_records}))")

    http_client = endpoint.http_client()
    if query == LfcQueryMethod.COMPUTE_CTL:
        http_client.offload_lfc()
    else:
        cur.execute("select get_local_cache_state()")
        lfc_state = cur.fetchall()[0][0]

    running = True
    n_prewarms = 0

    def workload():
        conn = endpoint.connect()
        cur = conn.cursor()
        n_transfers = 0
        while running:
            src = random.randint(1, n_records)
            dst = random.randint(1, n_records)
            cur.execute("update accounts set balance=balance-100 where id=%s", (src,))
            cur.execute("update accounts set balance=balance+100 where id=%s", (dst,))
            n_transfers += 1
        log.info(f"Number of transfers: {n_transfers}")

    def prewarm():
        conn = endpoint.connect()
        cur = conn.cursor()
        while running:
            cur.execute("alter system set neon.file_cache_size_limit='1MB'")
            cur.execute("select pg_reload_conf()")
            cur.execute("alter system set neon.file_cache_size_limit='1GB'")
            cur.execute("select pg_reload_conf()")

            if query == LfcQueryMethod.COMPUTE_CTL:
                http_client.prewarm_lfc()
            else:
                cur.execute("select prewarm_local_cache(%s)", (lfc_state,))

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

    time.sleep(20)

    running = False
    for t in workload_threads:
        t.join()
    prewarm_thread.join()

    cur.execute("select sum(balance) from accounts")
    total_balance = cur.fetchall()[0][0]
    assert total_balance == 0

    check_pinned_entries(cur)
    if query == LfcQueryMethod.COMPUTE_CTL:
        assert prom_parse(http_client) == {OFFLOAD_LABEL: 1, PREWARM_LABEL: n_prewarms}
