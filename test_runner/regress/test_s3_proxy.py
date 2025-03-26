import pytest
import os
import tempfile
import asyncio
import logging
from fixtures.remote_storage import (
    S3Storage,
    RemoteStorageKind,
    #available_remote_storages,
)
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)

pub_key = b"""
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJJ0zQDMmVhyqKScT1c582DrN89oGTk9+MYpjJpNVi7I=
-----END PUBLIC KEY-----
"""
priv_key = b"""-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIDeudDBdjOEq2gxyNpi8206poyFYm7wxGxG5YK9qO65j
-----END PRIVATE KEY-----
"""

#@pytest.mark.parametrize(
#    "remote_storage_kind",
#    available_remote_storages().remove(RemoteStorageKind.LOCAL_FS)
#)
@pytest.mark.asyncio
async def test_s3_proxy_smoke(
    neon_env_builder: NeonEnvBuilder#, remote_storage_kind: RemoteStorageKind
):
    storage: S3Storage = neon_env_builder.enable_s3_proxy_with(RemoteStorageKind.MOCK_S3)
    # TODO init_configs - don't create tenants
    env = neon_env_builder.init_start()

    fp = tempfile.NamedTemporaryFile(delete=False)
    fp.write(pub_key)
    fp.close()
    proxy_env = storage.access_env_vars()
    proxy_env["RUST_BACKTRACE"] = "1"
    proxy_env["RUST_LOG"] = "debug"

    storage.client.head_bucket(Bucket=storage.bucket_name)

    print(proxy_env, storage.endpoint)
    args = [
        f'{env.neon_binpath}/s3proxy',
        '--listen', '127.0.0.1:3000',
        '--bucket', storage.bucket_name,
        '--region', storage.bucket_region,
        '--endpoint', storage.endpoint,
        '--type', 'aws',
        '--pemfile', fp.name]
    proc = await asyncio.create_subprocess_exec(
       *args,
       stdout=asyncio.subprocess.PIPE,
       stderr=asyncio.subprocess.PIPE,
       env=proxy_env,
    )
    stdout, stderr = await proc.communicate()
    print(f'exited with {proc.returncode}')
    if stdout:
        print(f'[stdout]\n{stdout.decode()}')
    if stderr:
        print(f'[stderr]\n{stderr.decode()}')
    os.remove(fp.name)

    assert proc.returncode == 0
