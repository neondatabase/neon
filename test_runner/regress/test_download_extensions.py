import json
import os
from contextlib import closing
from io import BytesIO

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
)


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
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    assert env.ext_remote_storage is not None
    assert env.remote_storage_client is not None

    TEST_EXT_PATH = "v14/share/postgresql/extension/test_ext.control"
    BUCKET_PREFIX = "5314225671"  # this is the build number

    # 4. Upload test_ext.control file to the bucket
    # In the non-mock version this is done by CI/CD

    test_ext_file = BytesIO(
        b"""# mock extension
comment = 'This is a mock extension'
default_version = '1.0'
module_pathname = '$libdir/test_ext'
relocatable = true
    """
    )
    env.remote_storage_client.upload_fileobj(
        test_ext_file,
        env.ext_remote_storage.bucket_name,
        os.path.join(BUCKET_PREFIX, TEST_EXT_PATH),
    )

    # 5. Download file from the bucket to correct local location
    # Later this will be replaced by our rust code
    # resp = env.remote_storage_client.get_object(
    #     Bucket=env.ext_remote_storage.bucket_name, Key=os.path.join(BUCKET_PREFIX, TEST_EXT_PATH)
    # )
    # response = resp["Body"]
    # fname = f"pg_install/{TEST_EXT_PATH}"
    # with open(fname, "wb") as f:
    #     f.write(response.read())

    tenant, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant)

    remote_ext_config = json.dumps(
        {
            "bucket": env.ext_remote_storage.bucket_name,
            "region": "us-east-1",
            "endpoint": env.ext_remote_storage.endpoint,
            "prefix": BUCKET_PREFIX,
        }
    )

    # 6. Start endpoint and ensure that test_ext is present in select * from pg_available_extensions
    endpoint = env.endpoints.create_start(
        "test_file_download", tenant_id=tenant, remote_ext_config=remote_ext_config
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # test query: insert some values and select them
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            for i in range(100):
                cur.execute(f"insert into t values({i}, {2*i})")
            cur.execute("select * from t")
            log.info(cur.fetchall())

            # the real test query: check that test_ext is present
            cur.execute("SELECT * FROM pg_available_extensions")
            all_extensions = [x[0] for x in cur.fetchall()]
            log.info(all_extensions)
            assert "test_ext" in all_extensions
