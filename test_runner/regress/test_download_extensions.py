import os
import shutil
import time
from contextlib import closing
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.metrics import (
    PAGESERVER_GLOBAL_METRICS,
    PAGESERVER_PER_TENANT_METRICS,
    PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS,
    parse_metrics,
)
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until
from prometheus_client.samples import Sample


def test_file_download(neon_env_builder: NeonEnvBuilder):
    """Tests we can download a file"""
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_file_download",
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    with open("loggg", "w") as f:
        f.write(str(env.__dict__))

    TEST_EXT_PATH = "v15/share/extension/test_ext.control"

    # TODO: we shouldn't be using neon_env_builder.remote_storage_client,
    # we should pass the remote_storage_client to env in the builder.

    # 4. Upload test_ext.control file to the bucket
    # Later this will be done by CI/CD
    with open("test_ext.control", "rb") as data:
        neon_env_builder.remote_storage_client.upload_fileobj(
            data, neon_env_builder.remote_storage.bucket_name, TEST_EXT_PATH
        )

    # 5. Download file from the bucket to correct local location
    # Later this will be replaced by our rust code
    resp = neon_env_builder.remote_storage_client.get_object(
        Bucket=neon_env_builder.remote_storage.bucket_name, Key=TEST_EXT_PATH
    )
    content_length = resp["ResponseMetadata"]["HTTPHeaders"]["content-length"]
    # TODO: this is not the correct path, nor the correct data to write
    with open("pg_install/v15/lib/test_ext.control", "w") as f:
        f.write(str(resp))

    # env.neon_cli

    tenant, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant)
    # 6. Start endpoint and ensure that test_ext is present in select * from pg_available_extensions
    endpoint = env.endpoints.create_start("test_file_download", tenant_id=tenant)
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # cur.execute("CREATE EXTENSION test_load");
            # TODO: we should see the test_ext extension here
            other = cur.execute("SELECT * FROM pg_catalog.pg_tables;")
            whatsup = cur.execute("select * from pg_available_extensions;")
            with open("output.txt", "w") as f:
                f.write(str(whatsup) + str(other))
                # this is returning None????

    endpoint.stop()
    env.pageserver.http_client().tenant_detach(tenant)
