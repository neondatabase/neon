import json
import os
from contextlib import closing
from io import BytesIO

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    RemoteStorageKind,
)
from fixtures.pg_version import PgVersion
from fixtures.types import TenantId

BUCKET_PREFIX = "5314225671"  # in real setup this is the build_tag


def control_file_content(owner, i):
    output = f"""# mock {owner} extension{i}
comment = 'This is a mock extension'
default_version = '1.0'
module_pathname = '$libdir/test_ext{i}'
relocatable = true"""
    return output


def sql_file_content():
    output = """
            CREATE FUNCTION test_ext_add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
        """
    return output


# Prepare some mock extension files and upload them to the bucket
# returns a list of files that should be cleaned up after the test
def prepare_mock_ext_storage(
    pg_version: PgVersion,
    tenant_id: TenantId,
    pg_bin: PgBin,
    ext_remote_storage,
    remote_storage_client,
):
    private_prefix = str(tenant_id)
    PUB_EXT_ROOT = f"v{pg_version}/share/postgresql/extension"
    PRIVATE_EXT_ROOT = f"v{pg_version}/{private_prefix}/share/postgresql/extension"
    LOCAL_EXT_ROOT = f"pg_install/{PUB_EXT_ROOT}"

    PUB_LIB_ROOT = f"v{pg_version}/lib"
    PRIVATE_LIB_ROOT = f"v{pg_version}/{private_prefix}/lib"
    LOCAL_LIB_ROOT = f"{pg_bin.pg_lib_dir}/postgresql"

    log.info(
        f"""
            PUB_EXT_ROOT: {PUB_EXT_ROOT}
            PRIVATE_EXT_ROOT: {PRIVATE_EXT_ROOT}
            LOCAL_EXT_ROOT: {LOCAL_EXT_ROOT}
            PUB_LIB_ROOT: {PUB_LIB_ROOT}
            PRIVATE_LIB_ROOT: {PRIVATE_LIB_ROOT}
            LOCAL_LIB_ROOT: {LOCAL_LIB_ROOT}
            """
    )

    cleanup_files = []

    # Upload several test_ext{i}.control files to the bucket
    for i in range(5):
        # public extensions
        public_ext = BytesIO(bytes(control_file_content("public", i), "utf-8"))
        public_remote_name = f"{BUCKET_PREFIX}/{PUB_EXT_ROOT}/test_ext{i}.control"
        public_local_name = f"{LOCAL_EXT_ROOT}/test_ext{i}.control"

        # private extensions
        private_ext = BytesIO(bytes(control_file_content(str(tenant_id), i), "utf-8"))
        private_remote_name = f"{BUCKET_PREFIX}/{PRIVATE_EXT_ROOT}/private_ext{i}.control"
        private_local_name = f"{LOCAL_EXT_ROOT}/private_ext{i}.control"

        cleanup_files += [public_local_name, private_local_name]

        remote_storage_client.upload_fileobj(
            public_ext, ext_remote_storage.bucket_name, public_remote_name
        )
        remote_storage_client.upload_fileobj(
            private_ext, ext_remote_storage.bucket_name, private_remote_name
        )

    # Upload SQL file for the extension we're going to create
    sql_filename = "test_ext0--1.0.sql"
    test_sql_public_remote_path = f"{BUCKET_PREFIX}/{PUB_EXT_ROOT}/{sql_filename}"
    test_sql_local_path = f"{LOCAL_EXT_ROOT}/{sql_filename}"
    test_ext_sql_file = BytesIO(bytes(sql_file_content(), "utf-8"))
    remote_storage_client.upload_fileobj(
        test_ext_sql_file,
        ext_remote_storage.bucket_name,
        test_sql_public_remote_path,
    )
    cleanup_files += [test_sql_local_path]

    # upload some fake library files
    # TODO change it to test both public and private library paths
    for i in range(2):
        lib_filename = f"test_lib{i}.so"
        TEST_LIB_PATH = f"{PUB_LIB_ROOT}/{lib_filename}"
        lib_public_remote_path = f"{BUCKET_PREFIX}/{TEST_LIB_PATH}"
        lib_local_path = f"{LOCAL_LIB_ROOT}/{lib_filename}"
        test_lib_file = BytesIO(
            b"""
            111
            """
        )
        remote_storage_client.upload_fileobj(
            test_lib_file,
            ext_remote_storage.bucket_name,
            lib_public_remote_path,
        )
        log.info(f"lib_local_path: {lib_local_path}")
        cleanup_files += [lib_local_path]

    return cleanup_files


#
# Generate mock extension files and upload them to the bucket.
#
# Then check that compute nodes can download them and use them
# to CREATE EXTENSION and LOAD 'library.so'
#
# NOTE: you must have appropriate AWS credentials to run REAL_S3 test.
@pytest.mark.parametrize(
    "remote_storage_kind", [RemoteStorageKind.MOCK_S3, RemoteStorageKind.REAL_S3]
)
def test_remote_extensions(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
    pg_bin: PgBin,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_extensions",
        enable_remote_extensions=True,
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None
    assert env.remote_storage_client is not None

    # Prepare some mock extension files and upload them to the bucket
    cleanup_files = prepare_mock_ext_storage(
        pg_version,
        tenant_id,
        pg_bin,
        env.ext_remote_storage,
        env.remote_storage_client,
    )

    # TODO what region should we use for the test?
    region = "us-east-1"
    if remote_storage_kind == RemoteStorageKind.REAL_S3:
        region = "eu-central-1"

    remote_ext_config = json.dumps(
        {
            "bucket": env.ext_remote_storage.bucket_name,
            "region": region,
            "endpoint": env.ext_remote_storage.endpoint,
            "prefix": BUCKET_PREFIX,
        }
    )

    # Start a compute node and check that it can download the extensions
    # and use them to CREATE EXTENSION and LOAD 'library.so'
    #
    # This block is wrapped in a try/finally so that the downloaded files
    # are cleaned up even if the test fails
    try:
        endpoint = env.endpoints.create_start(
            "test_remote_extensions",
            tenant_id=tenant_id,
            remote_ext_config=remote_ext_config,
            config_lines=["log_min_messages=debug3"],
        )
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # Test query: check that test_ext0 was successfully downloaded
                cur.execute("SELECT * FROM pg_available_extensions")
                all_extensions = [x[0] for x in cur.fetchall()]
                log.info("ALEK*" * 100)
                log.info(all_extensions)
                for i in range(5):
                    assert f"test_ext{i}" in all_extensions
                    assert f"private_ext{i}" in all_extensions

                cur.execute("CREATE EXTENSION test_ext0")
                cur.execute("SELECT extname FROM pg_extension")
                all_extensions = [x[0] for x in cur.fetchall()]
                log.info(all_extensions)
                assert "test_ext0" in all_extensions

                # Try to load existing library file
                try:
                    cur.execute("LOAD 'test_lib0.so'")
                except Exception as e:
                    # expected to fail with
                    # could not load library ... test_ext.so: file too short
                    # because test_lib0.so is not real library file
                    log.info("LOAD test_lib0.so failed (expectedly): %s", e)
                    assert "file too short" in str(e)

                # Try to load existing library file without .so extension
                try:
                    cur.execute("LOAD 'test_lib1'")
                except Exception as e:
                    # expected to fail with
                    # could not load library ... test_lib1.so: file too short
                    # because test_lib1.so is not real library file
                    log.info("LOAD test_lib1 failed (expectedly): %s", e)
                    assert "file too short" in str(e)

                # Try to load non-existent library file
                try:
                    cur.execute("LOAD 'test_lib_fail.so'")
                except Exception as e:
                    # expected to fail because test_lib_fail.so is not found
                    log.info("LOAD test_lib_fail.so failed (expectedly): %s", e)
                    assert (
                        """could not access file "test_lib_fail.so": No such file or directory"""
                        in str(e)
                    )

    finally:
        # this is important because if the files aren't cleaned up then the test can
        # pass even without successfully downloading the files if a previous run (or
        # run with different type of remote storage) of the test did download the
        # files
        for file in cleanup_files:
            try:
                log.info(f"Deleting {file}")
                os.remove(file)
            except FileNotFoundError:
                log.info(f"{file} does not exist, so cannot be deleted")


# TODO
# @pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
# def test_remote_extensions_shared_preload_libraries(
#     neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind, pg_version: PgVersion
# ):
