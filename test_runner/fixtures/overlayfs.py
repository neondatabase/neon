from typing import Iterator
import psutil
from pathlib import Path


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
