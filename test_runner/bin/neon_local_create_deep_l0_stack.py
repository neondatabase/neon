"""
Script to creates a stack of L0 deltas each of which should have 1 Value::Delta per page in `data`,
in your running neon_local setup.

Use this bash setup to reset your neon_local environment.
The last line of this bash snippet will run this file here.
```
 export NEON_REPO_DIR=$PWD/.neon
 export NEON_BIN_DIR=$PWD/target/release
 $NEON_BIN_DIR/neon_local stop
 rm -rf $NEON_REPO_DIR
 $NEON_BIN_DIR/neon_local init
 cat >>  $NEON_REPO_DIR/pageserver_1/pageserver.toml <<"EOF"
 # customizations
 virtual_file_io_mode = "direct-rw"
 page_service_pipelining={mode="pipelined", max_batch_size=32, execution="concurrent-futures"}
 get_vectored_concurrent_io={mode="sidecar-task"}
EOF
 $NEON_BIN_DIR/neon_local start

 psql 'postgresql://localhost:1235/storage_controller' -c 'DELETE FROM tenant_shards'
 sed 's/.*get_vectored_concurrent_io.*/get_vectored_concurrent_io={mode="sidecar-task"}/' -i $NEON_REPO_DIR/pageserver_1/pageserver.toml
 $NEON_BIN_DIR/neon_local pageserver restart
 sleep 2
 $NEON_BIN_DIR/neon_local tenant create --set-default
 ./target/debug/neon_local endpoint stop foo
 rm -rf  $NEON_REPO_DIR/endpoints/foo
 ./target/debug/neon_local endpoint create foo
 echo 'full_page_writes=off' >>  $NEON_REPO_DIR/endpoints/foo/postgresql.conf
 ./target/debug/neon_local endpoint start foo

  pushd test_runner; poetry run python3 -m bin.neon_local_create_deep_l0_stack 10; popd
```
"""

import sys

import psycopg2
from fixtures.common_types import TenantShardId, TimelineId
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.makelayers.l0stack import L0StackShape, make_l0_stack_standalone

ps_http = PageserverHttpClient(port=9898, is_testing_enabled_or_skip=lambda: None)
vps_http = PageserverHttpClient(port=1234, is_testing_enabled_or_skip=lambda: None)

tenants = ps_http.tenant_list()
assert len(tenants) == 1
tenant_shard_id = TenantShardId.parse(tenants[0]["id"])

timlines = ps_http.timeline_list(tenant_shard_id)
assert len(timlines) == 1
timeline_id = TimelineId(timlines[0]["timeline_id"])

connstr = "postgresql://cloud_admin@localhost:55432/postgres"
conn = psycopg2.connect(connstr)

shape = L0StackShape(logical_table_size_mib=50, delta_stack_height=int(sys.argv[1]))

make_l0_stack_standalone(vps_http, ps_http, tenant_shard_id, timeline_id, conn, shape)
