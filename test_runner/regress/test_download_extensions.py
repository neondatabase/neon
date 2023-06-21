from contextlib import closing
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
)
import json


def test_file_download(neon_env_builder: NeonEnvBuilder):
    """
    Tests we can download a file
    First we set up the mock s3 bucket by uploading test_ext.control to the bucket
    Then, we download test_ext.control from the bucket to pg_install/v15/share/postgresql/extension/
    Finally, we list available extensions and assert that test_ext is present

    Right now we are downloading the file in python
    However, we have all the argument passing set up so that when an endpoint starts
    it knows about the bucket and can list_files in the bucket. This is written to ALEK_LIST_FILES.txt
    A good next step is to get rust to download the public_extensions control files to the correct place
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_file_download",
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    TEST_EXT_PATH = "v14/share/postgresql/extension/test_ext.control"

    # 4. Upload test_ext.control file to the bucket
    # In the non-mock version this is done by CI/CD
    with open("test_ext.control", "rb") as data:
        env.remote_storage_client.upload_fileobj(
            data, env.ext_remote_storage.bucket_name, TEST_EXT_PATH
        )

    # 5. Download file from the bucket to correct local location
    # Later this will be replaced by our rust code
    resp = env.remote_storage_client.get_object(
        Bucket=env.ext_remote_storage.bucket_name, Key=TEST_EXT_PATH
    )
    response = resp["Body"]
    fname = f"pg_install/{TEST_EXT_PATH}"
    with open(fname, "wb") as f:
        f.write(response.read())

    tenant, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant)

    remote_ext_config = json.dumps(
        {
            "bucket": env.ext_remote_storage.bucket_name,
            "region": "us-east-1",
            "endpoint": env.ext_remote_storage.endpoint,
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

    endpoint.stop()
    env.pageserver.http_client().tenant_detach(tenant)
