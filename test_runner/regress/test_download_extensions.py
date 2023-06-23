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

"""
TODO:
- **add tests with real S3 storage**
- libs/remote_storage/src/s3_bucket.rs TODO // TODO: if bucket prefix is empty,
    the folder is prefixed with a "/" I think. Is this desired?

- Handle LIBRARY exttensions
- how to add env variable EXT_REMOTE_STORAGE_S3_BUCKET?
"""


def ext_contents(owner, i):
    output = f"""# mock {owner} extension{i}
comment = 'This is a mock extension'
default_version = '1.0'
module_pathname = '$libdir/test_ext{i}'
relocatable = true"""
    return output


@pytest.mark.parametrize(
    "remote_storage_kind",
    [RemoteStorageKind.LOCAL_FS, RemoteStorageKind.MOCK_S3, RemoteStorageKind.REAL_S3],
)
def test_file_download(neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind):
    """
    Tests we can download a file
    First we set up the mock s3 bucket by uploading test_ext.control to the bucket
    Then, we download test_ext.control from the bucket to pg_install/v15/share/postgresql/extension/
    Finally, we list available extensions and assert that test_ext is present
    """

    if remote_storage_kind != RemoteStorageKind.MOCK_S3:
        # skip these for now
        return None

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

    NUM_EXT = 5
    PUB_EXT_ROOT = "v14/share/postgresql/extension"
    BUCKET_PREFIX = "5314225671"  # this is the build number
    cleanup_files = []

    # Upload test_ext{i}.control files to the bucket
    # Note: In real life this is done by CI/CD
    for i in range(NUM_EXT):
        # public extensions
        public_ext = BytesIO(bytes(ext_contents("public", i), "utf-8"))
        remote_name = f"{BUCKET_PREFIX}/{PUB_EXT_ROOT}/test_ext{i}.control"
        local_name = f"pg_install/{PUB_EXT_ROOT}/test_ext{i}.control"
        env.remote_storage_client.upload_fileobj(
            public_ext, env.ext_remote_storage.bucket_name, remote_name
        )
        cleanup_files.append(local_name)

        # private extensions
        private_ext = BytesIO(bytes(ext_contents(str(tenant_id), i), "utf-8"))
        remote_name = f"{BUCKET_PREFIX}/{str(tenant_id)}/private_ext{i}.control"
        local_name = f"pg_install/{PUB_EXT_ROOT}/private_ext{i}.control"
        env.remote_storage_client.upload_fileobj(
            private_ext, env.ext_remote_storage.bucket_name, remote_name
        )
        cleanup_files.append(local_name)

    # Rust will then download the control files from the bucket
    # our rust code should obtain the same result as the following:
    # env.remote_storage_client.get_object(
    #     Bucket=env.ext_remote_storage.bucket_name,
    #     Key=os.path.join(BUCKET_PREFIX, PUB_EXT_PATHS[0])
    # )["Body"].read()

    remote_ext_config = json.dumps(
        {
            "bucket": env.ext_remote_storage.bucket_name,
            "region": "us-east-1",
            "endpoint": env.ext_remote_storage.endpoint,
            "prefix": BUCKET_PREFIX,
        }
    )

    endpoint = env.endpoints.create_start(
        "test_file_download", tenant_id=tenant_id, remote_ext_config=remote_ext_config
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # example query: insert some values and select them
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            for i in range(100):
                cur.execute(f"insert into t values({i}, {2*i})")
            cur.execute("select * from t")
            log.info(cur.fetchall())

            # Test query: check that test_ext0 was successfully downloaded
            cur.execute("SELECT * FROM pg_available_extensions")
            all_extensions = [x[0] for x in cur.fetchall()]
            log.info(all_extensions)
            for i in range(NUM_EXT):
                assert f"test_ext{i}" in all_extensions
                assert f"private_ext{i}" in all_extensions

            # TODO: can create extension actually install an extension?
            # cur.execute("CREATE EXTENSION test_ext0")
            # log.info("**" * 100)
            # log.info(cur.fetchall())

    # cleanup downloaded extensions (TODO: the file names are quesionable here)
    for file in cleanup_files:
        try:
            log.info(f"Deleting {file}")
            os.remove(file)
        except FileNotFoundError:
            log.info(f"{file} does not exist, so cannot be deleted")
