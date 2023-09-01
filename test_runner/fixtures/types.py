import random
from functools import total_ordering
from typing import Any, Type, TypeVar, Union

T = TypeVar("T", bound="Id")


@total_ordering
class Lsn:
    """
    Datatype for an LSN. Internally it is a 64-bit integer, but the string
    representation is like "1/123abcd". See also pg_lsn datatype in Postgres
    """

    def __init__(self, x: Union[int, str]):
        if isinstance(x, int):
            self.lsn_int = x
        else:
            """Convert lsn from hex notation to int."""
            left, right = x.split("/")
            self.lsn_int = (int(left, 16) << 32) + int(right, 16)
        assert 0 <= self.lsn_int <= 0xFFFFFFFF_FFFFFFFF

    def __str__(self) -> str:
        """Convert lsn from int to standard hex notation."""
        return f"{(self.lsn_int >> 32):X}/{(self.lsn_int & 0xFFFFFFFF):X}"

    def __repr__(self) -> str:
        return f'Lsn("{str(self)}")'

    def __int__(self) -> int:
        return self.lsn_int

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int < other.lsn_int

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int == other.lsn_int

    # Returns the difference between two Lsns, in bytes
    def __sub__(self, other: Any) -> int:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int - other.lsn_int

    def __hash__(self) -> int:
        return hash(self.lsn_int)


@total_ordering
class Id:
    """
    Datatype for a Neon tenant and timeline IDs. Internally it's a 16-byte array, and
    the string representation is in hex. This corresponds to the Id / TenantId /
    TimelineIds in the Rust code.
    """

    def __init__(self, x: str):
        self.id = bytearray.fromhex(x)
        assert len(self.id) == 16

    def __str__(self) -> str:
        return self.id.hex()

    def __lt__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id < other.id

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(str(self.id))

    @classmethod
    def generate(cls: Type[T]) -> T:
        """Generate a random ID"""
        return cls(random.randbytes(16).hex())


class TenantId(Id):
    def __repr__(self) -> str:
        return f'`TenantId("{self.id.hex()}")'

    def __str__(self) -> str:
        return self.id.hex()


class TimelineId(Id):
    def __repr__(self) -> str:
        return f'TimelineId("{self.id.hex()}")'
