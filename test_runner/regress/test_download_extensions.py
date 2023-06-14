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

@pytest.mark.parametrize(
    "remote_storage_kind",
    # exercise both the code paths where remote_storage=None and remote_storage=Some(...)
    [RemoteStorageKind.MOCK_S3],
)
def test_file_download(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    """Tests we can download a file"""
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_file_download",
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_file_download", tenant_id=tenant)
    endpoint = env.endpoints.create_start("test_file_download", tenant_id=tenant)

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION test_load");
            # E psycopg2.errors.UndefinedFile: could not open extension control
            # file
            # "/home/alek/Desktop/neonX/pg_install/v14/share/postgresql/extension/test_load.control":
            # No such file or directory

    endpoint.stop()
    env.pageserver.http_client().tenant_detach(tenant)
