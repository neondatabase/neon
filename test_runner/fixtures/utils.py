import contextlib
import os
import re
import shutil
import subprocess
import tarfile
import time
from pathlib import Path
from typing import Any, Callable, List, Tuple, TypeVar

import allure  # type: ignore
from fixtures.log_helper import log
from psycopg2.extensions import cursor

Fn = TypeVar("Fn", bound=Callable[..., Any])


def get_self_dir() -> Path:
    """Get the path to the directory where this script lives."""
    return Path(__file__).resolve().parent


def subprocess_capture(capture_dir: Path, cmd: List[str], **kwargs: Any) -> str:
    """Run a process and capture its output

    Output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.
    Returns basepath for files with captured output.
    """
    assert type(cmd) is list
    base = os.path.basename(cmd[0]) + "_{}".format(global_counter())
    basepath = os.path.join(capture_dir, base)
    stdout_filename = basepath + ".stdout"
    stderr_filename = basepath + ".stderr"

    try:
        with open(stdout_filename, "w") as stdout_f:
            with open(stderr_filename, "w") as stderr_f:
                log.info(f'Capturing stdout to "{base}.stdout" and stderr to "{base}.stderr"')
                subprocess.run(cmd, **kwargs, stdout=stdout_f, stderr=stderr_f)
    finally:
        # Remove empty files if there is no output
        for filename in (stdout_filename, stderr_filename):
            if os.stat(filename).st_size == 0:
                os.remove(filename)

    return basepath


_global_counter = 0


def global_counter() -> int:
    """A really dumb global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
    _global_counter += 1
    return _global_counter


def print_gc_result(row):
    log.info("GC duration {elapsed} ms".format_map(row))
    log.info(
        "  total: {layers_total}, needed_by_cutoff {layers_needed_by_cutoff}, needed_by_pitr {layers_needed_by_pitr}"
        " needed_by_branches: {layers_needed_by_branches}, not_updated: {layers_not_updated}, removed: {layers_removed}".format_map(
            row
        )
    )


def etcd_path() -> Path:
    path_output = shutil.which("etcd")
    if path_output is None:
        raise RuntimeError("etcd not found in PATH")
    else:
        return Path(path_output)


def query_scalar(cur: cursor, query: str) -> Any:
    """
    It is a convenience wrapper to avoid repetitions
    of cur.execute(); cur.fetchone()[0]

    And this is mypy friendly, because without None
    check mypy says that Optional is not indexable.
    """
    cur.execute(query)
    var = cur.fetchone()
    assert var is not None
    return var[0]


# Traverse directory to get total size.
def get_dir_size(path: str) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            try:
                totalbytes += os.path.getsize(os.path.join(root, name))
            except FileNotFoundError:
                pass  # file could be concurrently removed

    return totalbytes


def get_timeline_dir_size(path: Path) -> int:
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
    return (
        int(key_parts[0], 16),
        int(key_parts[1], 16),
        int(lsn_parts[0], 16),
        int(lsn_parts[1], 16),
    )


def get_scale_for_db(size_mb: int) -> int:
    """Returns pgbench scale factor for given target db size in MB.

    Ref https://www.cybertec-postgresql.com/en/a-formula-to-calculate-pgbench-scaling-factor-for-target-db-size/
    """

    return round(0.06689 * size_mb - 0.5)


ATTACHMENT_NAME_REGEX = re.compile(
    r".+\.log|.+\.stderr|.+\.stdout|.+\.filediff|.+\.metrics|flamegraph\.svg|regression\.diffs|.+\.html"
)


def allure_attach_from_dir(dir: Path):
    """Attach all non-empty files from `dir` that matches `ATTACHMENT_NAME_REGEX` to Allure report"""

    for attachment in Path(dir).glob("**/*"):
        if ATTACHMENT_NAME_REGEX.fullmatch(attachment.name) and attachment.stat().st_size > 0:
            source = str(attachment)
            name = str(attachment.relative_to(dir))

            # compress files larger than 1Mb, they're hardly readable in a browser
            if attachment.stat().st_size > 1024 * 1024:
                source = f"{attachment}.tar.gz"
                with tarfile.open(source, "w:gz") as tar:
                    tar.add(attachment, arcname=attachment.name)
                name = f"{name}.tar.gz"

            if source.endswith(".tar.gz"):
                attachment_type = "application/gzip"
                extension = "tar.gz"
            elif source.endswith(".svg"):
                attachment_type = "image/svg+xml"
                extension = "svg"
            elif source.endswith(".html"):
                attachment_type = "text/html"
                extension = "html"
            else:
                attachment_type = "text/plain"
                extension = attachment.suffix.removeprefix(".")

            allure.attach.file(source, name, attachment_type, extension)


def start_in_background(
    command: list[str], cwd: Path, log_file_name: str, is_started: Fn
) -> subprocess.Popen[bytes]:
    """Starts a process, creates the logfile and redirects stderr and stdout there. Runs the start checks before the process is started, or errors."""

    log.info(f'Running command "{" ".join(command)}"')

    with open(cwd / log_file_name, "wb") as log_file:
        spawned_process = subprocess.Popen(command, stdout=log_file, stderr=log_file, cwd=cwd)
        error = None
        try:
            return_code = spawned_process.poll()
            if return_code is not None:
                error = f"expected subprocess to run but it exited with code {return_code}"
            else:
                attempts = 10
                try:
                    wait_until(
                        number_of_iterations=attempts,
                        interval=1,
                        func=is_started,
                    )
                except Exception:
                    error = f"Failed to get correct status from subprocess in {attempts} attempts"
        except Exception as e:
            error = f"expected subprocess to start but it failed with exception: {e}"

        if error is not None:
            log.error(error)
            spawned_process.kill()
            raise Exception(f"Failed to run subprocess as {command}, reason: {error}")

        log.info("subprocess spawned")
        return spawned_process


def wait_until(number_of_iterations: int, interval: float, func: Fn):
    """
    Wait until 'func' returns successfully, without exception. Returns the
    last return value from the function.
    """
    last_exception = None
    for i in range(number_of_iterations):
        try:
            res = func()
        except Exception as e:
            log.info("waiting for %s iteration %s failed", func, i + 1)
            last_exception = e
            time.sleep(interval)
            continue
        return res
    raise Exception("timed out while waiting for %s" % func) from last_exception
