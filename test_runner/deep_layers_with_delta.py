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
    ./target/release/neon_local pageserver restart
    ./target/debug/neon_local endpoint stop foo
    rm -rf .neon/endpoints/foo
    ./target/debug/neon_local endpoint create foo
    ./target/debug/neon_local endpoint start foo
    non-package-mode-py3.10christian@neon-hetzner-dev-christian:[~/src/neon/test_runner]: poetry run python3 deep_layers_with_delta.py

    
Layer files created:

christian@neon-hetzner-dev-christian:[~/src/neon]: ls -lrt .neon/pageserver_1/tenants/1cef6c5e3f94e02ac61d41d837fd6be9/timelines/0e1551891a0418657128d1b5ed2c16fb
total 64172
-rw-r--r-- 1 christian 23265280 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F1378-00000000014F13F1-v1-00000001
-rw-r--r-- 1 christian  9805824 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F13F1-0000000001C63B59-v1-00000002
-rw-r--r-- 1 christian  3334144 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001C63B59-0000000001EEFA19-v1-00000002
-rw-r--r-- 1 christian  4734976 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001EEFA19-000000000229BE69-v1-00000002
-rw-r--r-- 1 christian  6127616 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000229BE69-000000000275BF99-v1-00000002
-rw-r--r-- 1 christian  7520256 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000275BF99-0000000002D3C661-v1-00000002
-rw-r--r-- 1 christian  8863744 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002D3C661-000000000343D2A1-v1-00000002
-rw-r--r-- 1 christian   802816 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000343D2A1-00000000034D3589-v1-00000002
-rw-r--r-- 1 christian    24576 Dec 12 20:16 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000034D3589-00000000034D39E1-v1-00000002
-rw-r--r-- 1 christian   630784 Dec 12 20:17 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000034D39E1-0000000003507881-v1-00000002
-rw-r--r-- 1 christian   598016 Dec 12 20:17 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003507881-000000000353A0C9-v1-00000002
-rw-r--r-- 1 christian    49152 Dec 12 20:17 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000353A0C9-0000000003541269-v1-00000002
-rw-r--r-- 1 christian    24576 Dec 12 20:17 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003541269-00000000035412A1-v1-00000002
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
    "compaction_period": "0s",  # disable periodic compaction so we control when it happens
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
cur.execute("CREATE TABLE data(row char(100)) with (fillfactor=10)")
desired_size = 50 * 1024 * 1024  # 50MiB
need_pages = desired_size // 8192
need_rows = need_pages * 6
print(f"Need {need_pages} pages, {need_rows} rows")
cur.execute(f"INSERT INTO data SELECT i % 6 FROM generate_series(1, {need_rows}) as i")

# every iteration updates one tuple in each page
for i in range( 0, 20):
    print(i)
    cur.execute(f"UPDATE data set row = ((row::bigint + 1) % 6) where row::bigint % 6 = {i}")
    cur.execute("SELECT pg_current_wal_flush_lsn()")
    flush_lsn = Lsn(cur.fetchall()[0][0])

    while True:
        last_record = last_record_lsn(ps_http, tenant_id, timeline_id)
        if last_record >= flush_lsn:
            break
        time.sleep(0.1)

    ps_http.timeline_checkpoint(tenant_id, timeline_id)
    ps_http.timeline_compact(tenant_id, timeline_id)

