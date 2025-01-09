"""
Creates stack of L0 deltas each of which should have 1 Value::Delta per page in `data`,
in your running neon_local setup.

This showcases the need for concurrent IO.

I'm not completely sure about it because I don't yet understand the relation between
fillfactor and mvcc (does mvcc use the unfilled space in the page?).

I think there's a chance that some of the deltas are will_init .
Could be avoided with disabling fullpage writes.

TODO: concurrent IO will read beyond will_init and discard the data (I think).
Measure impact of that.

Some commands from shell history used in combination with this script:

 NEON_PAGESERVER_VALUE_RECONSTRUCT_IO_CONCURRENCY=futures-unordered ./target/release/neon_local pageserver restart
 NEON_PAGESERVER_VALUE_RECONSTRUCT_IO_CONCURRENCY=serial ./target/release/neon_local pageserver restart
 NEON_PAGESERVER_VALUE_RECONSTRUCT_IO_CONCURRENCY=parallel ./target/release/neon_local pageserver restart

starting from scrach:
    psql 'postgresql://localhost:1235/storage_controller' -c 'DELETE FROM tenant_shards'
    NEON_PAGESERVER_VALUE_RECONSTRUCT_IO_CONCURRENCY=... ./target/release/neon_local pageserver restart
    ./target/debug/neon_local endpoint stop foo
    rm -rf .neon/endpoints/foo
    ./target/debug/neon_local endpoint create foo
    echo 'full_page_writes=off' >> .neon/endpoints/foo/postgresql.conf
    ./target/debug/neon_local endpoint start foo
    non-package-mode-py3.10christian@neon-hetzner-dev-christian:[~/src/neon/test_runner]: poetry run python3 deep_layers_with_delta.py
    Need 6400 pages, 38400 rows
    0
    modified rows 6400
    1
    modified rows 6400
    ...
    19
    modified rows 6400

Layer files created:

christian@neon-hetzner-dev-christian:[~/src/neon]: ls -hlrt .neon/pageserver_1/tenants/*/timelines/*
total 67M
-rw-r--r-- 1 christian  23M Dec 13 12:38 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F1378-00000000014F13F1-v1-00000001
-rw-r--r-- 1 christian 9.3M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F13F1-0000000001C4D021-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001C4D021-0000000001DC51F1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001DC51F1-0000000001F3D3C1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001F3D3C1-00000000020B55A1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000020B55A1-000000000222D771-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000222D771-00000000023A5941-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000023A5941-000000000251DB11-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000251DB11-0000000002695D19-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002695D19-000000000280DEE9-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000280DEE9-00000000029860D1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000029860D1-0000000002AFE2A1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002AFE2A1-0000000002C76471-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002C76471-0000000002DEE679-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002DEE679-0000000002F66849-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002F66849-00000000030DEA29-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000030DEA29-0000000003256BF9-v1-00000001
-rw-r--r-- 1 christian 2.0M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003256BF9-00000000033CEE39-v1-00000001
-rw-r--r-- 1 christian 1.7M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000033CEE39-0000000003512B91-v1-00000001
-rw-r--r-- 1 christian 1.4M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003512B91-0000000003633D91-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003633D91-00000000037AE2E9-v1-00000001
-rw-r--r-- 1 christian    0 Dec 13 12:42 ephemeral-22

"""


from pathlib import Path
import time
from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.pageserver.http import PageserverHttpClient

ps_http = PageserverHttpClient(port=9898, is_testing_enabled_or_skip=lambda: None)
vps_http = PageserverHttpClient(port=1234, is_testing_enabled_or_skip=lambda: None)

tenants = ps_http.tenant_list()
assert len(tenants) == 1
tenant_id = tenants[0]["id"]

timlines = ps_http.timeline_list(tenant_id)
assert len(timlines) == 1
timeline_id = timlines[0]["timeline_id"]

config = {
    "gc_period": "0s",  # disable periodic gc
    "checkpoint_timeout": "10 years",
    "compaction_period": "1h",  # doesn't matter, but 0 value will kill walredo every 10s
    "compaction_threshold": 100000, # we just want L0s
    "compaction_target_size": 134217728,
    "checkpoint_distance": 268435456,
    "image_creation_threshold": 100000, # we just want L0s
}

vps_http.set_tenant_config(tenant_id, config)

connstr = "postgresql://cloud_admin@localhost:55432/postgres"

import psycopg2

def last_record_lsn(
    pageserver_http_client: PageserverHttpClient,
    tenant: TenantId | TenantShardId,
    timeline: TimelineId,
) -> Lsn:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail["last_record_lsn"]
    assert isinstance(lsn_str, str)
    return Lsn(lsn_str)

conn = psycopg2.connect(connstr)
conn.autocommit = True
cur = conn.cursor()

# each tuple is 23 (header) + 100 bytes = 123 bytes
# page header si 24 bytes
# 8k page size
# (8k-24bytes) / 123 bytes = 63 tuples per page
# set fillfactor to 10 to have 6 tuples per page
cur.execute("DROP TABLE IF EXISTS data")
cur.execute("CREATE TABLE data(id bigint, row char(92)) with (fillfactor=10)")
desired_size = 50 * 1024 * 1024  # 50MiB
need_pages = desired_size // 8192
need_rows = need_pages * 6
print(f"Need {need_pages} pages, {need_rows} rows")
cur.execute(f"INSERT INTO data SELECT i,'row'||i FROM generate_series(1, {need_rows}) as i")

# every iteration updates one tuple in each page
delta_stack_height = 20
for i in range( 0, delta_stack_height):
    print(i)
    cur.execute(f"UPDATE data set row = row||',u' where id % 6 = {i%6}")
    print("modified rows", cur.rowcount)
    cur.execute("SELECT pg_current_wal_flush_lsn()")
    flush_lsn = Lsn(cur.fetchall()[0][0])

    while True:
        last_record = last_record_lsn(ps_http, tenant_id, timeline_id)
        if last_record >= flush_lsn:
            break
        time.sleep(0.1)

    ps_http.timeline_checkpoint(tenant_id, timeline_id)
    ps_http.timeline_compact(tenant_id, timeline_id)

