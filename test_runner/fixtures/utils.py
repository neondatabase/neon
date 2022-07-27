import contextlib
import os
import pathlib
import shutil
import subprocess
from pathlib import Path

from typing import Any, List, Tuple
from fixtures.log_helper import log


def get_self_dir() -> str:
    """ Get the path to the directory where this script lives. """
    return os.path.dirname(os.path.abspath(__file__))


def subprocess_capture(capture_dir: str, cmd: List[str], **kwargs: Any) -> str:
    """ Run a process and capture its output

    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + '_{}'.format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + '.stdout'
    stderr_filename = basepath + '.stderr'

    with open(stdout_filename, 'w') as stdout_f:
        with open(stderr_filename, 'w') as stderr_f:
            log.info('(capturing output to "{}.stdout")'.format(base))
            subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)

    return basepath


_global_counter = 0


def global_counter() -> int:
    """ A really dumb global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter


def lsn_to_hex(num: int) -> str:
    """ Convert lsn from int to standard hex notation. """
    return "{:X}/{:X}".format(num >> 32, num & 0xffffffff)


def lsn_from_hex(lsn_hex: str) -> int:
    """ Convert lsn from hex notation to int. """
    l, r = lsn_hex.split('/')
    return (int(l, 16) << 32) + int(r, 16)


def print_gc_result(row):
    log.info("GC duration {elapsed} ms".format_map(row))
    log.info(
        "  total: {layers_total}, needed_by_cutoff {layers_needed_by_cutoff}, needed_by_pitr {layers_needed_by_pitr}"
        " needed_by_branches: {layers_needed_by_branches}, not_updated: {layers_not_updated}, removed: {layers_removed}"
        .format_map(row))


def etcd_path() -> Path:
    path_output = shutil.which("etcd")
    if path_output is None:
        raise RuntimeError('etcd not found in PATH')
    else:
        return Path(path_output)


# Traverse directory to get total size.
def get_dir_size(path: str) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            try:
                totalbytes += os.path.getsize(os.path.join(root, name))
            except FileNotFoundError as e:
                pass  # file could be concurrently removed

    return totalbytes


def get_timeline_dir_size(path: pathlib.Path) -> int:
    """Get the timeline directory's total size, which only counts the layer files' size."""
    sz = 0
    for dir_entry in path.iterdir():
        with contextlib.suppress(Exception):
            # file is an image layer
            _ = parse_image_layer(dir_entry.name)
            sz += dir_entry.stat().st_size
            continue

        with contextlib.suppress(Exception):
            # file is a delta layer
            _ = parse_delta_layer(dir_entry.name)
            sz += dir_entry.stat().st_size
            continue
    return sz


def parse_image_layer(f_name: str) -> Tuple[int, int, int]:
    """Parse an image layer file name. Return key start, key end, and snapshot lsn"""
    parts = f_name.split("__")
    key_parts = parts[0].split("-")
    return int(key_parts[0], 16), int(key_parts[1], 16), int(parts[1], 16)


def parse_delta_layer(f_name: str) -> Tuple[int, int, int, int]:
    """Parse a delta layer file name. Return key start, key end, lsn start, and lsn end"""
    parts = f_name.split("__")
    key_parts = parts[0].split("-")
    lsn_parts = parts[1].split("-")
    return int(key_parts[0], 16), int(key_parts[1], 16), int(lsn_parts[0], 16), int(lsn_parts[1], 16)
