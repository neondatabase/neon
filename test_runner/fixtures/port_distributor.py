import re
import socket
from contextlib import closing
from typing import Dict, Union

from fixtures.log_helper import log


def can_bind(host: str, port: int) -> bool:
    """
    Check whether a host:port is available to bind for listening

    Inspired by the can_bind() perl function used in Postgres tests, in
    vendor/postgres-v14/src/test/perl/PostgresNode.pm
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        # TODO: The pageserver and safekeepers don't use SO_REUSEADDR at the
        # moment. If that changes, we should use start using SO_REUSEADDR here
        # too, to allow reusing ports more quickly.
        # See https://github.com/neondatabase/neon/issues/801
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            sock.bind((host, port))
            sock.listen()
            return True
        except socket.error:
            log.info(f"Port {port} is in use, skipping")
            return False
        finally:
            sock.close()


class PortDistributor:
    def __init__(self, base_port: int, port_number: int):
        self.iterator = iter(range(base_port, base_port + port_number))
        self.port_map: Dict[int, int] = {}

    def get_port(self) -> int:
        for port in self.iterator:
            if can_bind("localhost", port):
                return port
        raise RuntimeError(
            "port range configured for test is exhausted, consider enlarging the range"
        )

    def replace_with_new_port(self, value: Union[int, str]) -> Union[int, str]:
        """
        Returns a new port for a port number in a string (like "localhost:1234") or int.
        Replacements are memorised, so a substitution for the same port is always the same.
        """

        # TODO: replace with structural pattern matching for Python >= 3.10
        if isinstance(value, int):
            return self._replace_port_int(value)

        if isinstance(value, str):
            return self._replace_port_str(value)

        raise TypeError(f"unsupported type {type(value)} of {value=}")

    def _replace_port_int(self, value: int) -> int:
        known_port = self.port_map.get(value)
        if known_port is None:
            known_port = self.port_map[value] = self.get_port()

        return known_port

    def _replace_port_str(self, value: str) -> str:
        # Use regex to find port in a string
        # urllib.parse.urlparse produces inconvenient results for cases without scheme like "localhost:5432"
        # See https://bugs.python.org/issue27657
        ports = re.findall(r":(\d+)(?:/|$)", value)
        assert len(ports) == 1, f"can't find port in {value}"
        port_int = int(ports[0])

        return value.replace(f":{port_int}", f":{self._replace_port_int(port_int)}")
