import os
import shutil
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
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_extensions",
        enable_remote_extensions=True,
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        log.info("Uploading test files to mock bucket")

        def upload_test_file(from_path, to_path):
            assert env.ext_remote_storage is not None  # satisfy mypy
            assert env.remote_storage_client is not None  # satisfy mypy
            with open(
                f"test_runner/regress/data/extension_test/v{pg_version}/{from_path}", "rb"
            ) as f:
                env.remote_storage_client.upload_fileobj(
                    f,
                    env.ext_remote_storage.bucket_name,
                    f"ext/v{pg_version}/{to_path}",
                )

        upload_test_file("ext_index.json", "ext_index.json")
        upload_test_file("anon.tar.gz", "extensions/anon.tar.gz")
        upload_test_file("embedding.tar.gz", "extensions/embedding.tar.gz")

    assert env.ext_remote_storage is not None  # satisfy mypy
    assert env.remote_storage_client is not None  # satisfy mypy
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
                # TODO: check that we cant't download custom extensions for other tenant ids

                # check that we can download public extension
                cur.execute("CREATE EXTENSION embedding")
                cur.execute("SELECT extname FROM pg_extension")
                assert "embedding" in [x[0] for x in cur.fetchall()]

                # check that we can download private extension
                try:
                    cur.execute("CREATE EXTENSION anon")
                except Exception as err:
                    log.info("error creating anon extension")
                    assert "pgcrypto" in str(err), "unexpected error creating anon extension"

                # TODO: try to load libraries as well

    finally:
        # Cleaning up downloaded files is important for local tests
        # or else one test could reuse the files from another test or another test run
        cleanup_files = [
            "embedding.tar.gz",
            "anon.tar.gz",
            f"pg_install/v{pg_version}/lib/postgresql/anon.so",
            f"pg_install/v{pg_version}/lib/postgresql/embedding.so",
            f"pg_install/v{pg_version}/share/postgresql/extension/anon.control",
            f"pg_install/v{pg_version}/share/postgresql/extension/embedding--0.1.0.sql",
            f"pg_install/v{pg_version}/share/postgresql/extension/embedding.control",
        ]
        cleanup_folders = [
            "extensions",
            f"pg_install/v{pg_version}/share/postgresql/extension/anon",
        ]
        for file in cleanup_files:
            try:
                os.remove(file)
                log.info(f"removed file {file}")
            except Exception as err:
                log.info(f"error removing file {file}: {err}")
        for folder in cleanup_folders:
            try:
                shutil.rmtree(folder)
                log.info(f"removed folder {folder}")
            except Exception as err:
                log.info(f"error removing file {file}: {err}")
