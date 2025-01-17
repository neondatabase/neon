from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fixtures.common_types import KEY_MAX, KEY_MIN, Key, Lsn

if TYPE_CHECKING:
    from typing import Any


@dataclass
class IndexLayerMetadata:
    file_size: int
    generation: int


@dataclass(frozen=True)
class ImageLayerName:
    lsn: Lsn
    key_start: Key
    key_end: Key

    def to_str(self):
        ret = (
            f"{self.key_start.as_int():036X}-{self.key_end.as_int():036X}__{self.lsn.as_int():016X}"
        )
        assert self == parse_layer_file_name(ret)
        return ret


@dataclass(frozen=True)
class DeltaLayerName:
    lsn_start: Lsn
    lsn_end: Lsn
    key_start: Key
    key_end: Key

    def is_l0(self) -> bool:
        return self.key_start == KEY_MIN and self.key_end == KEY_MAX

    def to_str(self) -> str:
        ret = f"{self.key_start.as_int():036X}-{self.key_end.as_int():036X}__{self.lsn_start.as_int():016X}-{self.lsn_end.as_int():016X}"
        assert self == parse_layer_file_name(ret)
        return ret


LayerName = ImageLayerName | DeltaLayerName


class InvalidFileName(Exception):
    pass


IMAGE_LAYER_FILE_NAME = re.compile(
    "^([A-F0-9]{36})-([A-F0-9]{36})__([A-F0-9]{16})(-v1-[a-f0-9]{8})?$"
)


def parse_image_layer(f_name: str) -> tuple[int, int, int]:
    """Parse an image layer file name. Return key start, key end, and snapshot lsn"""

    match = IMAGE_LAYER_FILE_NAME.match(f_name)
    if match is None:
        raise InvalidFileName(f"'{f_name}' is not an image layer filename")

    return int(match.group(1), 16), int(match.group(2), 16), int(match.group(3), 16)


DELTA_LAYER_FILE_NAME = re.compile(
    "^([A-F0-9]{36})-([A-F0-9]{36})__([A-F0-9]{16})-([A-F0-9]{16})(-v1-[a-f0-9]{8})?$"
)


def parse_delta_layer(f_name: str) -> tuple[int, int, int, int]:
    """Parse a delta layer file name. Return key start, key end, lsn start, and lsn end"""
    match = DELTA_LAYER_FILE_NAME.match(f_name)
    if match is None:
        raise InvalidFileName(f"'{f_name}' is not an delta layer filename")

    return (
        int(match.group(1), 16),
        int(match.group(2), 16),
        int(match.group(3), 16),
        int(match.group(4), 16),
    )


def parse_layer_file_name(file_name: str) -> LayerName:
    try:
        key_start, key_end, lsn = parse_image_layer(file_name)
        return ImageLayerName(lsn=Lsn(lsn), key_start=Key(key_start), key_end=Key(key_end))
    except InvalidFileName:
        pass

    try:
        key_start, key_end, lsn_start, lsn_end = parse_delta_layer(file_name)
        return DeltaLayerName(
            lsn_start=Lsn(lsn_start),
            lsn_end=Lsn(lsn_end),
            key_start=Key(key_start),
            key_end=Key(key_end),
        )
    except InvalidFileName:
        pass

    raise InvalidFileName("neither image nor delta layer")


def is_future_layer(layer_file_name: LayerName, disk_consistent_lsn: Lsn):
    """
    Determines if this layer file is considered to be in future meaning we will discard these
    layers during timeline initialization from the given disk_consistent_lsn.
    """
    if isinstance(layer_file_name, ImageLayerName) and layer_file_name.lsn > disk_consistent_lsn:
        return True
    elif (
        isinstance(layer_file_name, DeltaLayerName)
        and layer_file_name.lsn_end > disk_consistent_lsn + 1
    ):
        return True
    else:
        return False


@dataclass
class IndexPartDump:
    layer_metadata: dict[LayerName, IndexLayerMetadata]
    disk_consistent_lsn: Lsn

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> IndexPartDump:
        return IndexPartDump(
            layer_metadata={
                parse_layer_file_name(n): IndexLayerMetadata(v["file_size"], v["generation"])
                for n, v in d["layer_metadata"].items()
            },
            disk_consistent_lsn=Lsn(d["disk_consistent_lsn"]),
        )
