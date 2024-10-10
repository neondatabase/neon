from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import psutil

if TYPE_CHECKING:
    from collections.abc import Iterator


def iter_mounts_beneath(topdir: Path) -> Iterator[Path]:
    """
    Iterate over the overlayfs mounts beneath the specififed `topdir`.
    The `topdir` itself isn't considered.
    """
    for part in psutil.disk_partitions(all=True):
        if part.fstype == "overlay":
            mountpoint = Path(part.mountpoint)
            if topdir in mountpoint.parents:
                yield mountpoint
