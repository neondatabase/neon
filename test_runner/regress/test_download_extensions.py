import json
import os
from contextlib import closing
from io import BytesIO

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
)
from fixtures.pg_version import PgVersion

"""
TODO Alek:
Calling list_files on a non-existing path returns [] (expectedly) but then
causes the program to crash somehow (for both real and mock s3 storage)
stderr: command failed: unexpected compute status: Empty

- real s3 tests: I think the paths were slightly different than I was expecting
-  clean up the junk I put in the bucket
- libs/remote_storage/src/s3_bucket.rs TODO // TODO: if bucket prefix is empty,
    the folder is prefixed with a "/" I think. Is this desired?
"""


"""
for local test running with mock s3: make sure the following environment variables are set
export AWS_ACCESS_KEY_ID='test'
export AWS_SECRET_ACCESS_KEY='test'
export AWS_SECURITY_TOKEN='test'
export AWS_SESSION_TOKEN='test'
export AWS_DEFAULT_REGION='us-east-1'
"""


def ext_contents(owner, i):
    output = f"""# mock {owner} extension{i}
comment = 'This is a mock extension'
default_version = '1.0'
module_pathname = '$libdir/test_ext{i}'
relocatable = true"""
    return output


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.MOCK_S3])
def test_file_download(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind, pg_version: PgVersion
):
    """
    Tests we can download a file
    First we set up the mock s3 bucket by uploading test_ext.control to the bucket
    Then, we download test_ext.control from the bucket to pg_install/v15/share/postgresql/extension/
    Finally, we list available extensions and assert that test_ext is present
    """

    ## temporarily disable RemoteStorageKind.REAL_S3

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_file_download",
        enable_remote_extensions=True,
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None
    assert env.remote_storage_client is not None

    PUB_EXT_ROOT = f"v{pg_version}/share/postgresql/extension"
    BUCKET_PREFIX = "5314225671"  # this is the build number
    cleanup_files = []

    # Upload test_ext{i}.control files to the bucket (for MOCK_S3)
    # Note: In real life this is done by CI/CD
    for i in range(5):
        # public extensions
        public_ext = BytesIO(bytes(ext_contents("public", i), "utf-8"))
        public_remote_name = f"{BUCKET_PREFIX}/{PUB_EXT_ROOT}/test_ext{i}.control"
        public_local_name = f"pg_install/{PUB_EXT_ROOT}/test_ext{i}.control"
        # private extensions
        BytesIO(bytes(ext_contents(str(tenant_id), i), "utf-8"))
        f"{BUCKET_PREFIX}/{str(tenant_id)}/private_ext{i}.control"
        private_local_name = f"pg_install/{PUB_EXT_ROOT}/private_ext{i}.control"

        cleanup_files += [public_local_name, private_local_name]

        if remote_storage_kind == RemoteStorageKind.MOCK_S3:
            env.remote_storage_client.upload_fileobj(
                public_ext, env.ext_remote_storage.bucket_name, public_remote_name
            )
            # env.remote_storage_client.upload_fileobj(
            #     private_ext, env.ext_remote_storage.bucket_name, private_remote_name
            # )

    TEST_EXT_SQL_PATH = f"v{pg_version}/share/postgresql/extension/test_ext0--1.0.sql"
    test_ext_sql_file = BytesIO(
        b"""
            CREATE FUNCTION test_ext_add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
        """
    )
    env.remote_storage_client.upload_fileobj(
        test_ext_sql_file,
        env.ext_remote_storage.bucket_name,
        os.path.join(BUCKET_PREFIX, TEST_EXT_SQL_PATH),
    )

    # upload some fake library files
    for i in range(2):
        TEST_LIB_PATH = f"v{pg_version}/lib/test_ext{i}.so"
        test_lib_file = BytesIO(
            b"""
            111
            """
        )
        # TODO: maybe if we are using REAL_S3 storage, we should not upload files
        # or at least, maybe we should delete them afterwards
        env.remote_storage_client.upload_fileobj(
            test_lib_file,
            env.ext_remote_storage.bucket_name,
            os.path.join(BUCKET_PREFIX, TEST_LIB_PATH),
        )

    tenant, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant)

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

    try:
        endpoint = env.endpoints.create_start(
            "test_file_download",
            tenant_id=tenant,
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
                    # assert f"private_ext{i}" in all_extensions

                cur.execute("CREATE EXTENSION test_ext0")
                cur.execute("SELECT extname FROM pg_extension")
                all_extensions = [x[0] for x in cur.fetchall()]
                log.info(all_extensions)
                assert "test_ext0" in all_extensions

                try:
                    cur.execute("LOAD 'test_ext0.so'")
                except Exception as e:
                    # expected to fail with
                    # could not load library ... test_ext.so: file too short
                    # because test_ext.so is not real library file
                    log.info("LOAD test_ext0.so failed (expectedly): %s", e)
                    assert "file too short" in str(e)

                # TODO add more test cases:
                # - try to load non-existing library
    finally:
        # cleanup downloaded extensions
        # TODO: clean up downloaded libraries too
        # TODO: make sure this runs even if the test fails
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
