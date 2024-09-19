from typing import Iterator

import pytest

from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import MockS3Server


@pytest.fixture(scope="session")
def mock_s3_server(port_distributor: PortDistributor) -> Iterator[MockS3Server]:
    mock_s3_server = MockS3Server(port_distributor.get_port())
    yield mock_s3_server
    mock_s3_server.kill()
