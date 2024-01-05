from typing import Iterator
import psutil
from pathlib import Path


def iter_mounts_beneath(topdir: Path) -> Iterator[Path]:
    for part in psutil.disk_partitions(all=True):
        if part.fstype == "overlay":
            mountpoint = Path(part.mountpoint)
            if topdir in mountpoint.parents:
                yield mountpoint
