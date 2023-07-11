import os
from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
    available_s3_storages,
)
from fixtures.pg_version import PgVersion

# Generate mock extension files and upload them to the mock bucket.
#
# NOTE: You must have appropriate AWS credentials to run REAL_S3 test.
# It may also be necessary to set the following environment variables for MOCK_S3 test:
#   export AWS_ACCESS_KEY_ID='test' #   export AWS_SECRET_ACCESS_KEY='test'
#   export AWS_SECURITY_TOKEN='test' #   export AWS_SESSION_TOKEN='test'
#   export AWS_DEFAULT_REGION='us-east-1'


@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
def test_remote_extensions(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
):
    # TODO: SKIP for now, infra not ready yet
    if remote_storage_kind == RemoteStorageKind.REAL_S3:
        return None

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

    # For MOCK_S3 we upload some test files. for REAL_S3 we use the files created in CICD
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        log.info("Uploading test files to mock bucket")
        with open("test_runner/regress/data/extension_test/ext_index.json", "rb") as f:
            env.remote_storage_client.upload_fileobj(
                f,
                env.ext_remote_storage.bucket_name,
                f"ext/v{pg_version}/ext_index.json",
            )
        with open("test_runner/regress/data/extension_test/anon.tar.gz", "rb") as f:
            env.remote_storage_client.upload_fileobj(
                f,
                env.ext_remote_storage.bucket_name,
                f"ext/v{pg_version}/extensions/anon.tar.gz",
            )
        with open("test_runner/regress/data/extension_test/embedding.tar.gz", "rb") as f:
            env.remote_storage_client.upload_fileobj(
                f,
                env.ext_remote_storage.bucket_name,
                f"ext/v{pg_version}/extensions/embedding.tar.gz",
            )

    try:
        # Start a compute node and check that it can download the extensions
        # and use them to CREATE EXTENSION and LOAD
        endpoint = env.endpoints.create_start(
            "test_remote_extensions",
            tenant_id=tenant_id,
            remote_ext_config=env.ext_remote_storage.to_string(),
            # config_lines=["log_min_messages=debug3"],
        )
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # Check that appropriate control files were downloaded
                cur.execute("SELECT * FROM pg_available_extensions")
                all_extensions = [x[0] for x in cur.fetchall()]
                log.info(all_extensions)
                assert "anon" in all_extensions
                assert "embedding" in all_extensions
                # TODO: check that we don't have download custom extensions for other tenant ids
                # TODO: not sure how private extension will work with REAL_S3 test. can we rig the tenant id?

                # check that we can download public extension
                cur.execute("CREATE EXTENSION embedding")
                cur.execute("SELECT extname FROM pg_extension")
                assert "embedding" in [x[0] for x in cur.fetchall()]

                # check that we can download private extension
                # TODO: this will fail locally because we don't have the required dependencies
                cur.execute("CREATE EXTENSION anon")
                cur.execute("SELECT extname FROM pg_extension")
                assert "embedding" in [x[0] for x in cur.fetchall()]

                # TODO: should we try libraries too?

    finally:
        cleanup_files = ["embedding.tar.gz", "anon.tar.gz"]
        # for file in cleanup_files:
        #     os.remove(file)
        log.info("For now, please manually cleanup ", cleanup_files)
