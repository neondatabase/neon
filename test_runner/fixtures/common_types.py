from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum
from functools import total_ordering
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import override

if TYPE_CHECKING:
    from typing import Any

    T = TypeVar("T", bound="Id")


DEFAULT_WAL_SEG_SIZE = 16 * 1024 * 1024


@total_ordering
class Lsn:
    """
    Datatype for an LSN. Internally it is a 64-bit integer, but the string
    representation is like "1/0123abcd". See also pg_lsn datatype in Postgres
    """

    def __init__(self, x: int | str):
        if isinstance(x, int):
            self.lsn_int = x
        else:
            """Convert lsn from hex notation to int."""
            left, right = x.split("/")
            self.lsn_int = (int(left, 16) << 32) + int(right, 16)
        assert 0 <= self.lsn_int <= 0xFFFFFFFF_FFFFFFFF

    @override
    def __str__(self) -> str:
        """Convert lsn from int to standard hex notation."""
        return f"{(self.lsn_int >> 32):X}/{(self.lsn_int & 0xFFFFFFFF):X}"

    @override
    def __repr__(self) -> str:
        return f'Lsn("{str(self)}")'

    def __int__(self) -> int:
        return self.lsn_int

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int < other.lsn_int

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Lsn):
            raise NotImplementedError
        return self.lsn_int > other.lsn_int

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int == other.lsn_int

    # Returns the difference between two Lsns, in bytes
    def __sub__(self, other: object) -> int:
        if not isinstance(other, Lsn):
            return NotImplemented
        return self.lsn_int - other.lsn_int

    def __add__(self, other: int | Lsn) -> Lsn:
        if isinstance(other, int):
            return Lsn(self.lsn_int + other)
        elif isinstance(other, Lsn):
            return Lsn(self.lsn_int + other.lsn_int)
        else:
            raise NotImplementedError

    @override
    def __hash__(self) -> int:
        return hash(self.lsn_int)

    def as_int(self) -> int:
        return self.lsn_int

    def segment_lsn(self, seg_sz: int = DEFAULT_WAL_SEG_SIZE) -> Lsn:
        return Lsn(self.lsn_int - (self.lsn_int % seg_sz))

    def segno(self, seg_sz: int = DEFAULT_WAL_SEG_SIZE) -> int:
        return self.lsn_int // seg_sz

    def segment_name(self, seg_sz: int = DEFAULT_WAL_SEG_SIZE) -> str:
        segno = self.segno(seg_sz)
        # The filename format is 00000001XXXXXXXX000000YY, where XXXXXXXXYY is segno in hex.
        # XXXXXXXX is the higher 8 hex digits of segno
        high_bits = segno >> 8
        # YY is the lower 2 hex digits of segno
        low_bits = segno & 0xFF
        return f"00000001{high_bits:08X}000000{low_bits:02X}"


@dataclass(frozen=True)
class Key:
    key_int: int

    def as_int(self) -> int:
        return self.key_int


KEY_MAX = Key(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
KEY_MIN = Key(0)


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

    @override
    def __str__(self) -> str:
        return self.id.hex()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id < other.id

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id == other.id

    @override
    def __hash__(self) -> int:
        return hash(str(self.id))

    @classmethod
    def generate(cls: type[T]) -> T:
        """Generate a random ID"""
        return cls(random.randbytes(16).hex())


class TenantId(Id):
    @override
    def __repr__(self) -> str:
        return f'`TenantId("{self.id.hex()}")'

    @override
    def __str__(self) -> str:
        return self.id.hex()


class NodeId(Id):
    @override
    def __repr__(self) -> str:
        return f'`NodeId("{self.id.hex()}")'

    @override
    def __str__(self) -> str:
        return self.id.hex()


class TimelineId(Id):
    @override
    def __repr__(self) -> str:
        return f'TimelineId("{self.id.hex()}")'

    @override
    def __str__(self) -> str:
        return self.id.hex()


@dataclass
class TenantTimelineId:
    tenant_id: TenantId
    timeline_id: TimelineId

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TenantTimelineId:
        return TenantTimelineId(
            tenant_id=TenantId(d["tenant_id"]),
            timeline_id=TimelineId(d["timeline_id"]),
        )


@dataclass
class ShardIndex:
    shard_number: int
    shard_count: int

    # cf impl Display for ShardIndex
    @override
    def __str__(self) -> str:
        return f"{self.shard_number:02x}{self.shard_count:02x}"

    @classmethod
    def parse(cls: type[ShardIndex], input: str) -> ShardIndex:
        assert len(input) == 4
        return cls(
            shard_number=int(input[0:2], 16),
            shard_count=int(input[2:4], 16),
        )


class TenantShardId:
    def __init__(self, tenant_id: TenantId, shard_number: int, shard_count: int):
        self.tenant_id = tenant_id
        self.shard_number = shard_number
        self.shard_count = shard_count
        assert self.shard_number < self.shard_count or self.shard_count == 0

    @classmethod
    def parse(cls: type[TenantShardId], input: str) -> TenantShardId:
        if len(input) == 32:
            return cls(
                tenant_id=TenantId(input),
                shard_number=0,
                shard_count=0,
            )
        elif len(input) == 37:
            return cls(
                tenant_id=TenantId(input[0:32]),
                shard_number=int(input[33:35], 16),
                shard_count=int(input[35:37], 16),
            )
        else:
            raise ValueError(f"Invalid TenantShardId '{input}'")

    @override
    def __str__(self):
        if self.shard_count > 0:
            return f"{self.tenant_id}-{self.shard_number:02x}{self.shard_count:02x}"
        else:
            # Unsharded case: equivalent of Rust TenantShardId::unsharded(tenant_id)
            return str(self.tenant_id)

    @property
    def shard_index(self) -> ShardIndex:
        return ShardIndex(self.shard_number, self.shard_count)

    @override
    def __repr__(self):
        return self.__str__()

    def _tuple(self) -> tuple[TenantId, int, int]:
        return (self.tenant_id, self.shard_number, self.shard_count)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._tuple() < other._tuple()

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._tuple() == other._tuple()

    @override
    def __hash__(self) -> int:
        return hash(self._tuple())


class TimelineArchivalState(StrEnum):
    ARCHIVED = "Archived"
    UNARCHIVED = "Unarchived"
