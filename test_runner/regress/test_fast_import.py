from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from fixtures.fast_import import FastImport
from fixtures.log_helper import log


#
# Create ancestor branches off the main branch.
#
def test_fast_import(port_distributor, test_output_dir: Path, neon_binpath: Path, pg_distrib_dir: Path):

    # TODO:
    # 1. fast_import fixture
    # 2. maybe run vanilla postgres and insert some data into it
    # 3. run fast_import in interactive mode in the background
    #  - wait for "interactive mode" message
    # 4. run some query against the imported data (in running postgres inside fast_import, port is known)

    # Maybe test with pageserver?
    # 1. run whole neon env
    # 2. create timeline with some s3 path???
    # 3. run fast_import with s3 prefix
    # 4. ??? mock http where pageserver will report progress
    # 5. run compute on this timeline and check if data is there


    fast_import = FastImport(
        None,
        neon_binpath,
        pg_distrib_dir / "v16",
    )
    workdir = Path(tempfile.mkdtemp())
    cmd = fast_import.run(
        Path(tempfile.mkdtemp()),
        port_distributor.get_port(),
        "postgresql://gleb:O9UM4tPHEafC@ep-autumn-rain-a3smlr75.eu-central-1.aws.neon.tech/neondb?sslmode=require",
        None,
    )

    # dump stdout & stderr into test log dir
    with open(test_output_dir / "fast_import.stdout", "w") as f:
        f.write(cmd.stdout)
    with open(test_output_dir / "fast_import.stderr", "w") as f:
        f.write(cmd.stderr)

    log.info('Written logs to %s', test_output_dir)

    # clean workdir
    shutil.rmtree(workdir)


    # env = neon_env_builder.init_start()
    # pageserver_http = env.pageserver.http_client()
    # # Override defaults: 4M checkpoint_distance, disable background compaction and gc.
    # tenant, _ = env.create_tenant(
    #     conf={
    #         "checkpoint_distance": "4194304",
    #         "gc_period": "0s",
    #         "compaction_period": "0s",
    #     }
    # )
    #
    # failpoint = "flush-frozen-pausable"
    #
    # pageserver_http.configure_failpoints((failpoint, "sleep(10000)"))
    #
    # endpoint_branch0 = env.endpoints.create_start("main", tenant_id=tenant)
    # branch0_cur = endpoint_branch0.connect().cursor()
    # branch0_timeline = TimelineId(query_scalar(branch0_cur, "SHOW neon.timeline_id"))
    # log.info(f"b0 timeline {branch0_timeline}")
    #
    # # Create table, and insert 100k rows.
    # branch0_lsn = query_scalar(branch0_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"b0 at lsn {branch0_lsn}")
    #
    # branch0_cur.execute("CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)")
    # branch0_cur.execute(
    #     """
    #     INSERT INTO foo
    #         SELECT '00112233445566778899AABBCCDDEEFF' || ':branch0:' || g
    #         FROM generate_series(1, 100000) g
    # """
    # )
    # lsn_100 = query_scalar(branch0_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"LSN after 100k rows: {lsn_100}")
    #
    # # Create branch1.
    # env.create_branch(
    #     "branch1", ancestor_branch_name="main", ancestor_start_lsn=lsn_100, tenant_id=tenant
    # )
    # endpoint_branch1 = env.endpoints.create_start("branch1", tenant_id=tenant)
    #
    # branch1_cur = endpoint_branch1.connect().cursor()
    # branch1_timeline = TimelineId(query_scalar(branch1_cur, "SHOW neon.timeline_id"))
    # log.info(f"b1 timeline {branch1_timeline}")
    #
    # branch1_lsn = query_scalar(branch1_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"b1 at lsn {branch1_lsn}")
    #
    # # Insert 100k rows.
    # branch1_cur.execute(
    #     """
    #     INSERT INTO foo
    #         SELECT '00112233445566778899AABBCCDDEEFF' || ':branch1:' || g
    #         FROM generate_series(1, 100000) g
    # """
    # )
    # lsn_200 = query_scalar(branch1_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"LSN after 200k rows: {lsn_200}")
    #
    # # Create branch2.
    # env.create_branch(
    #     "branch2", ancestor_branch_name="branch1", ancestor_start_lsn=lsn_200, tenant_id=tenant
    # )
    # endpoint_branch2 = env.endpoints.create_start("branch2", tenant_id=tenant)
    # branch2_cur = endpoint_branch2.connect().cursor()
    #
    # branch2_timeline = TimelineId(query_scalar(branch2_cur, "SHOW neon.timeline_id"))
    # log.info(f"b2 timeline {branch2_timeline}")
    #
    # branch2_lsn = query_scalar(branch2_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"b2 at lsn {branch2_lsn}")
    #
    # # Insert 100k rows.
    # branch2_cur.execute(
    #     """
    #     INSERT INTO foo
    #         SELECT '00112233445566778899AABBCCDDEEFF' || ':branch2:' || g
    #         FROM generate_series(1, 100000) g
    # """
    # )
    # lsn_300 = query_scalar(branch2_cur, "SELECT pg_current_wal_insert_lsn()")
    # log.info(f"LSN after 300k rows: {lsn_300}")
    #
    # # Run compaction on branch1.
    # compact = f"compact {tenant} {branch1_timeline}"
    # log.info(compact)
    # pageserver_http.timeline_compact(tenant, branch1_timeline)
    #
    # assert query_scalar(branch0_cur, "SELECT count(*) FROM foo") == 100000
    #
    # assert query_scalar(branch1_cur, "SELECT count(*) FROM foo") == 200000
    #
    # assert query_scalar(branch2_cur, "SELECT count(*) FROM foo") == 300000
    #
    # pageserver_http.configure_failpoints((failpoint, "off"))