import json
import os
from contextlib import closing
from io import BytesIO

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
)

"""
TODO:
- Handle not just Shared Extensions but also other types

- how to add env variable EXT_REMOTE_STORAGE_S3_BUCKET?
- add tests for my thing with real S3 storage
- libs/remote_storage/src/s3_bucket.rs TODO // TODO: if bucket prefix is empty,
    the folder is prefixed with a "/" I think. Is this desired?
"""


def ext_contents(owner, i):
    output = f"""# mock {owner} extension{i}
comment = 'This is a mock extension'
default_version = '1.0'
module_pathname = '$libdir/test_ext{i}'
relocatable = true"""
    return output


def test_file_download(neon_env_builder: NeonEnvBuilder):
    """
    Tests we can download a file
    First we set up the mock s3 bucket by uploading test_ext.control to the bucket
    Then, we download test_ext.control from the bucket to pg_install/v15/share/postgresql/extension/
    Finally, we list available extensions and assert that test_ext is present
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
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
    PUB_EXT_PATHS = [f"v14/share/postgresql/extension/test_ext{i}.control" for i in range(NUM_EXT)]
    BUCKET_PREFIX = "5314225671"  # this is the build number

    # Upload test_ext{i}.control files to the bucket
    # Note: In real life this is done by CI/CD
    for i in range(NUM_EXT):
        # public extensions
        test_ext_file = BytesIO(bytes(ext_contents("public", i), "utf-8"))
        env.remote_storage_client.upload_fileobj(
            test_ext_file,
            env.ext_remote_storage.bucket_name,
            os.path.join(BUCKET_PREFIX, PUB_EXT_PATHS[i]),
        )
        # private extensions
        test_ext_file = BytesIO(bytes(ext_contents(str(tenant_id), i), "utf-8"))
        env.remote_storage_client.upload_fileobj(
            test_ext_file,
            env.ext_remote_storage.bucket_name,
            os.path.join(BUCKET_PREFIX, f"{str(tenant_id)}/private_ext{i}.control"),
        )

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

            # TODO: cleanup downloaded extensions
