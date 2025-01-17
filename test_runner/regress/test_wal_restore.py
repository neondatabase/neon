from __future__ import annotations

import sys
import tarfile
import tempfile
from pathlib import Path

import pytest
import zstandard
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
)
from fixtures.pageserver.utils import (
    list_prefix,
    remote_storage_delete_key,
    timeline_delete_wait_completed,
)
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import LocalFsStorage, S3Storage, s3_storage
from mypy_boto3_s3.type_defs import (
    ObjectTypeDef,
)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="restore_from_wal.sh supports only Linux",
)
def test_wal_restore(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    base_dir: Path,
    pg_distrib_dir: Path,
):
    env = neon_env_builder.init_start()
    env.create_branch("test_wal_restore")
    endpoint = env.endpoints.create_start("test_wal_restore")
    endpoint.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = TenantId(endpoint.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(endpoint.safe_psql("show neon.timeline_id")[0][0])
    env.pageserver.stop()
    port = port_distributor.get_port()
    data_dir = test_output_dir / "pgsql.restored"
    with VanillaPostgres(
        data_dir, PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version), port
    ) as restored:
        pg_bin.run_capture(
            [
                str(base_dir / "libs" / "utils" / "scripts" / "restore_from_wal.sh"),
                str(pg_distrib_dir / f"v{env.pg_version}/bin"),
                str(
                    test_output_dir
                    / "repo"
                    / "safekeepers"
                    / "sk1"
                    / str(tenant_id)
                    / str(timeline_id)
                ),
                str(data_dir),
                str(port),
                env.pg_version,
            ]
        )
        restored.start()
        assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]


def decompress_zstd(
    input_file_name: Path,
    output_dir: Path,
):
    log.info(f"decompressing zstd to: {output_dir}")
    output_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    with tempfile.TemporaryFile(suffix=".tar") as temp:
        decompressor = zstandard.ZstdDecompressor()
        with open(input_file_name, "rb") as input_file:
            decompressor.copy_stream(input_file, temp)
        temp.seek(0)
        with tarfile.open(fileobj=temp) as tfile:
            tfile.extractall(path=output_dir)


def test_wal_restore_initdb(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    base_dir: Path,
    pg_distrib_dir: Path,
):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")
    endpoint.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    original_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    env.pageserver.stop()
    port = port_distributor.get_port()
    data_dir = test_output_dir / "pgsql.restored"

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    initdb_zst_path = (
        env.pageserver_remote_storage.timeline_path(tenant_id, timeline_id) / "initdb.tar.zst"
    )

    decompress_zstd(initdb_zst_path, data_dir)
    with VanillaPostgres(
        data_dir, PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version), port, init=False
    ) as restored:
        pg_bin.run_capture(
            [
                str(base_dir / "libs" / "utils" / "scripts" / "restore_from_wal_initdb.sh"),
                str(pg_distrib_dir / f"v{env.pg_version}/bin"),
                str(
                    test_output_dir
                    / "repo"
                    / "safekeepers"
                    / "sk1"
                    / str(tenant_id)
                    / str(timeline_id)
                ),
                str(data_dir),
                str(port),
                env.pg_version,
            ]
        )
        restored.start()
        restored_lsn = Lsn(
            restored.safe_psql("SELECT pg_current_wal_flush_lsn()", user="cloud_admin")[0][0]
        )
        log.info(f"original lsn: {original_lsn}, restored lsn: {restored_lsn}")
        assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]


@pytest.mark.parametrize("broken_tenant", [True, False])
def test_wal_restore_http(neon_env_builder: NeonEnvBuilder, broken_tenant: bool):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")
    endpoint.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    ps_client = env.pageserver.http_client()

    if broken_tenant:
        env.pageserver.allowed_errors.append(
            r".* Changing Active tenant to Broken state, reason: broken from test"
        )
        ps_client.tenant_break(tenant_id)

    # Mark the initdb archive for preservation
    ps_client.timeline_preserve_initdb_archive(tenant_id, timeline_id)

    # shut down the endpoint and delete the timeline from the pageserver
    endpoint.stop()

    assert isinstance(env.pageserver_remote_storage, S3Storage)

    if broken_tenant:
        ps_client.tenant_detach(tenant_id)
        objects: list[ObjectTypeDef] = list_prefix(
            env.pageserver_remote_storage, f"tenants/{tenant_id}/timelines/{timeline_id}/"
        ).get("Contents", [])
        for obj in objects:
            obj_key = obj["Key"]
            if "initdb-preserved.tar.zst" in obj_key:
                continue
            log.info(f"Deleting key from remote storage: {obj_key}")
            remote_storage_delete_key(env.pageserver_remote_storage, obj_key)
            pass

        ps_client.tenant_attach(tenant_id, generation=10)
    else:
        timeline_delete_wait_completed(ps_client, tenant_id, timeline_id)

    # issue the restoration command
    ps_client.timeline_create(
        tenant_id=tenant_id,
        new_timeline_id=timeline_id,
        existing_initdb_timeline_id=timeline_id,
        pg_version=env.pg_version,
    )

    # the table is back now!
    restored = env.endpoints.create_start("main")
    assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]
