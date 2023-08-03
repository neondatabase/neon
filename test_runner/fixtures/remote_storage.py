import enum
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

from fixtures.log_helper import log


class MockS3Server:
    """
    Starts a mock S3 server for testing on a port given, errors if the server fails to start or exits prematurely.
    Relies that `poetry` and `moto` server are installed, since it's the way the tests are run.

    Also provides a set of methods to derive the connection properties from and the method to kill the underlying server.
    """

    def __init__(
        self,
        port: int,
    ):
        self.port = port

        # XXX: do not use `shell=True` or add `exec ` to the command here otherwise.
        # We use `self.subprocess.kill()` to shut down the server, which would not "just" work in Linux
        # if a process is started from the shell process.
        self.subprocess = subprocess.Popen(["poetry", "run", "moto_server", "s3", f"-p{port}"])
        error = None
        try:
            return_code = self.subprocess.poll()
            if return_code is not None:
                error = f"expected mock s3 server to run but it exited with code {return_code}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        except Exception as e:
            error = f"expected mock s3 server to start but it failed with exception: {e}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        if error is not None:
            log.error(error)
            self.kill()
            raise RuntimeError("failed to start s3 mock server")

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def region(self) -> str:
        return "us-east-1"

    def access_key(self) -> str:
        return "test"

    def secret_key(self) -> str:
        return "test"

    def kill(self):
        self.subprocess.kill()


@enum.unique
class RemoteStorageKind(str, enum.Enum):
    LOCAL_FS = "local_fs"
    MOCK_S3 = "mock_s3"
    REAL_S3 = "real_s3"
    # Pass to tests that are generic to remote storage
    # to ensure the test pass with or without the remote storage
    NOOP = "noop"


def available_remote_storages() -> List[RemoteStorageKind]:
    remote_storages = [RemoteStorageKind.LOCAL_FS, RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE") is not None:
        remote_storages.append(RemoteStorageKind.REAL_S3)
        log.info("Enabling real s3 storage for tests")
    else:
        log.info("Using mock implementations to test remote storage")
    return remote_storages


def available_s3_storages() -> List[RemoteStorageKind]:
    remote_storages = [RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE") is not None:
        remote_storages.append(RemoteStorageKind.REAL_S3)
        log.info("Enabling real s3 storage for tests")
    else:
        log.info("Using mock implementations to test remote storage")
    return remote_storages


@dataclass
class LocalFsStorage:
    root: Path


@dataclass
class S3Storage:
    bucket_name: str
    bucket_region: str
    access_key: str
    secret_key: str
    endpoint: Optional[str] = None
    prefix_in_bucket: Optional[str] = ""

    def access_env_vars(self) -> Dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
        }

    def to_string(self) -> str:
        return json.dumps(
            {
                "bucket": self.bucket_name,
                "region": self.bucket_region,
                "endpoint": self.endpoint,
                "prefix": self.prefix_in_bucket,
            }
        )


RemoteStorage = Union[LocalFsStorage, S3Storage]


# serialize as toml inline table
def remote_storage_to_toml_inline_table(remote_storage: RemoteStorage) -> str:
    if isinstance(remote_storage, LocalFsStorage):
        remote_storage_config = f"local_path='{remote_storage.root}'"
    elif isinstance(remote_storage, S3Storage):
        remote_storage_config = f"bucket_name='{remote_storage.bucket_name}',\
            bucket_region='{remote_storage.bucket_region}'"

        if remote_storage.prefix_in_bucket is not None:
            remote_storage_config += f",prefix_in_bucket='{remote_storage.prefix_in_bucket}'"

        if remote_storage.endpoint is not None:
            remote_storage_config += f",endpoint='{remote_storage.endpoint}'"
    else:
        raise Exception("invalid remote storage type")

    return f"{{{remote_storage_config}}}"


class RemoteStorageUsers(enum.Flag):
    PAGESERVER = enum.auto()
    SAFEKEEPER = enum.auto()
