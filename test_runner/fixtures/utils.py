import contextlib
import enum
import json
import os
import re
import subprocess
import tarfile
import threading
import time
from hashlib import sha256
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlencode

import allure
import zstandard
from psycopg2.extensions import cursor

from fixtures.log_helper import log
from fixtures.pageserver.common_types import (
    parse_delta_layer,
    parse_image_layer,
)

if TYPE_CHECKING:
    from fixtures.neon_fixtures import PgBin
from collections import OrderedDict

from allpairspy import AllPairs

from fixtures.common_types import TimelineId

Fn = TypeVar("Fn", bound=Callable[..., Any])
COMPONENT_BINARIES = {
    "storage_controller": ["storage_controller"],
    "storage_broker": ["storage_broker"],
    "compute": ["compute_ctl"],
    "safekeeper": ["safekeeper"],
    "pageserver": ["pageserver", "pagectl"],
}


def subprocess_capture(
    capture_dir: Path,
    cmd: List[str],
    *,
    check=False,
    echo_stderr=False,
    echo_stdout=False,
    capture_stdout=False,
    timeout=None,
    with_command_header=True,
    **popen_kwargs: Any,
) -> Tuple[str, Optional[str], int]:
    """Run a process and bifurcate its output to files and the `log` logger

    stderr and stdout are always captured in files.  They are also optionally
    echoed to the log (echo_stderr, echo_stdout), and/or captured and returned
    (capture_stdout).

    File output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.

    Returns 3-tuple of:
     - The base path for output files
     - Captured stdout, or None
     - The exit status of the process
    """
    assert isinstance(cmd, list)
    base_cmd = os.path.basename(cmd[0])
    base = f"{base_cmd}_{global_counter()}"
    basepath = os.path.join(capture_dir, base)
    stdout_filename = f"{basepath}.stdout"
    stderr_filename = f"{basepath}.stderr"

    # Since we will stream stdout and stderr concurrently, need to do it in a thread.
    class OutputHandler(threading.Thread):
        def __init__(self, in_file, out_file, echo: bool, capture: bool):
            super().__init__()
            self.in_file = in_file
            self.out_file = out_file
            self.echo = echo
            self.capture = capture
            self.captured = ""

        def run(self):
            first = with_command_header
            for line in self.in_file:
                if first:
                    # do this only after receiving any input so that we can
                    # keep deleting empty files, or leave it out completly if
                    # it was unwanted (using the file as input later for example)
                    first = False
                    # prefix the files with the command line so that we can
                    # later understand which file is for what command
                    self.out_file.write((f"# {' '.join(cmd)}\n\n").encode("utf-8"))

                # Only bother decoding if we are going to do something more than stream to a file
                if self.echo or self.capture:
                    string = line.decode(encoding="utf-8", errors="replace")

                    if self.echo:
                        log.info(string.strip())

                    if self.capture:
                        self.captured += string

                self.out_file.write(line)

    captured = None
    try:
        with open(stdout_filename, "wb") as stdout_f:
            with open(stderr_filename, "wb") as stderr_f:
                log.info(f'Capturing stdout to "{base}.stdout" and stderr to "{base}.stderr"')

                p = subprocess.Popen(
                    cmd,
                    **popen_kwargs,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                stdout_handler = OutputHandler(
                    p.stdout, stdout_f, echo=echo_stdout, capture=capture_stdout
                )
                stdout_handler.start()
                stderr_handler = OutputHandler(p.stderr, stderr_f, echo=echo_stderr, capture=False)
                stderr_handler.start()

                r = p.wait(timeout=timeout)

                stdout_handler.join()
                stderr_handler.join()

                if check and r != 0:
                    raise subprocess.CalledProcessError(r, " ".join(cmd))

                if capture_stdout:
                    captured = stdout_handler.captured
    finally:
        # Remove empty files if there is no output
        for filename in (stdout_filename, stderr_filename):
            if os.stat(filename).st_size == 0:
                os.remove(filename)

    return (basepath, captured, r)


_global_counter = 0
_global_counter_lock = threading.Lock()


def global_counter() -> int:
    """A really dumb but thread-safe global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter, _global_counter_lock
    with _global_counter_lock:
        _global_counter += 1
        return _global_counter


def print_gc_result(row: Dict[str, Any]):
    log.info("GC duration {elapsed} ms".format_map(row))
    log.info(
        "  total: {layers_total}, needed_by_cutoff {layers_needed_by_cutoff}, needed_by_pitr {layers_needed_by_pitr}"
        " needed_by_branches: {layers_needed_by_branches}, not_updated: {layers_not_updated}, removed: {layers_removed}".format_map(
            row
        )
    )


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
def get_dir_size(path: Path) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, _dirs, files in os.walk(path):
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
    return sz


def get_scale_for_db(size_mb: int) -> int:
    """Returns pgbench scale factor for given target db size in MB.

    Ref https://www.cybertec-postgresql.com/en/a-formula-to-calculate-pgbench-scaling-factor-for-target-db-size/
    """

    return round(0.06689 * size_mb - 0.5)


ATTACHMENT_NAME_REGEX: re.Pattern = re.compile(  # type: ignore[type-arg]
    r"regression\.(diffs|out)|.+\.(?:log|stderr|stdout|filediff|metrics|html|walredo)"
)


def allure_attach_from_dir(dir: Path, preserve_database_files: bool = False):
    """Attach all non-empty files from `dir` that matches `ATTACHMENT_NAME_REGEX` to Allure report"""

    if preserve_database_files:
        zst_file = dir.with_suffix(".tar.zst")
        with zst_file.open("wb") as zst:
            cctx = zstandard.ZstdCompressor()
            with cctx.stream_writer(zst) as compressor:
                with tarfile.open(fileobj=compressor, mode="w") as tar:
                    tar.add(dir, arcname="")
        allure.attach.file(zst_file, "everything.tar.zst", "application/zstd", "tar.zst")

    for attachment in Path(dir).glob("**/*"):
        if ATTACHMENT_NAME_REGEX.fullmatch(attachment.name) and attachment.stat().st_size > 0:
            name = str(attachment.relative_to(dir))

            # compress files that are larger than 1Mb, they're hardly readable in a browser
            if attachment.stat().st_size > 1024**2:
                compressed = attachment.with_suffix(".zst")

                cctx = zstandard.ZstdCompressor()
                with attachment.open("rb") as fin, compressed.open("wb") as fout:
                    cctx.copy_stream(fin, fout)

                name = f"{name}.zst"
                attachment = compressed

            source = str(attachment)
            if source.endswith(".gz"):
                attachment_type = "application/gzip"
                extension = "gz"
            elif source.endswith(".zst"):
                attachment_type = "application/zstd"
                extension = "zst"
            elif source.endswith(".svg"):
                attachment_type = "image/svg+xml"
                extension = "svg"
            elif source.endswith(".html"):
                attachment_type = "text/html"
                extension = "html"
            elif source.endswith(".walredo"):
                attachment_type = "application/octet-stream"
                extension = "walredo"
            else:
                attachment_type = "text/plain"
                extension = attachment.suffix.removeprefix(".")

            allure.attach.file(source, name, attachment_type, extension)


GRAFANA_URL = "https://neonprod.grafana.net"
GRAFANA_EXPLORE_URL = f"{GRAFANA_URL}/explore"
GRAFANA_TIMELINE_INSPECTOR_DASHBOARD_URL = f"{GRAFANA_URL}/d/8G011dlnk/timeline-inspector"
LOGS_STAGING_DATASOURCE_ID = "xHHYY0dVz"


def allure_add_grafana_links(host: str, timeline_id: TimelineId, start_ms: int, end_ms: int):
    """Add links to server logs in Grafana to Allure report"""
    links = {}
    # We expect host to be in format like ep-divine-night-159320.us-east-2.aws.neon.build
    endpoint_id, region_id, _ = host.split(".", 2)

    expressions = {
        "compute logs": f'{{app="compute-node-{endpoint_id}", neon_region="{region_id}"}}',
        "k8s events": f'{{job="integrations/kubernetes/eventhandler"}} |~ "name=compute-node-{endpoint_id}-"',
        "console logs": f'{{neon_service="console", neon_region="{region_id}"}} | json | endpoint_id = "{endpoint_id}"',
        "proxy logs": f'{{neon_service="proxy-scram", neon_region="{region_id}"}}',
    }

    params: Dict[str, Any] = {
        "datasource": LOGS_STAGING_DATASOURCE_ID,
        "queries": [
            {
                "expr": "<PUT AN EXPRESSION HERE>",
                "refId": "A",
                "datasource": {"type": "loki", "uid": LOGS_STAGING_DATASOURCE_ID},
                "editorMode": "code",
                "queryType": "range",
            }
        ],
        "range": {
            "from": str(start_ms),
            "to": str(end_ms),
        },
    }
    for name, expr in expressions.items():
        params["queries"][0]["expr"] = expr
        query_string = urlencode({"orgId": 1, "left": json.dumps(params)})
        links[name] = f"{GRAFANA_EXPLORE_URL}?{query_string}"

    timeline_qs = urlencode(
        {
            "orgId": 1,
            "var-environment": "victoria-metrics-aws-dev",
            "var-timeline_id": timeline_id,
            "var-endpoint_id": endpoint_id,
            "var-log_datasource": "grafanacloud-neonstaging-logs",
            "from": start_ms,
            "to": end_ms,
        }
    )
    link = f"{GRAFANA_TIMELINE_INSPECTOR_DASHBOARD_URL}?{timeline_qs}"
    links["Timeline Inspector"] = link

    for name, link in links.items():
        allure.dynamic.link(link, name=name)
        log.info(f"{name}: {link}")


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


WaitUntilRet = TypeVar("WaitUntilRet")


def wait_until(
    number_of_iterations: int,
    interval: float,
    func: Callable[[], WaitUntilRet],
    show_intermediate_error=False,
) -> WaitUntilRet:
    """
    Wait until 'func' returns successfully, without exception. Returns the
    last return value from the function.
    """
    last_exception = None
    for i in range(number_of_iterations):
        try:
            res = func()
        except Exception as e:
            log.info("waiting for %s iteration %s failed: %s", func, i + 1, e)
            last_exception = e
            if show_intermediate_error:
                log.info(e)
            time.sleep(interval)
            continue
        return res
    raise Exception("timed out while waiting for %s" % func) from last_exception


def assert_eq(a, b) -> None:
    assert a == b


def assert_gt(a, b) -> None:
    assert a > b


def assert_ge(a, b) -> None:
    assert a >= b


def run_pg_bench_small(pg_bin: "PgBin", connstr: str):
    """
    Fast way to populate data.
    For more layers consider combining with these tenant settings:
    {
        "checkpoint_distance": 1024 ** 2,
        "image_creation_threshold": 100,
    }
    """
    pg_bin.run(["pgbench", "-i", "-I dtGvp", "-s1", connstr])


def humantime_to_ms(humantime: str) -> float:
    """
    Converts Rust humantime's output string to milliseconds.

    humantime_to_ms("1h 1ms 406us") -> 3600001.406
    """

    unit_multiplier_map = {
        "ns": 1e-6,
        "us": 1e-3,
        "ms": 1,
        "s": 1e3,
        "m": 1e3 * 60,
        "h": 1e3 * 60 * 60,
    }
    matcher = re.compile(rf"^(\d+)({'|'.join(unit_multiplier_map.keys())})$")
    total_ms = 0.0

    if humantime == "0":
        return total_ms

    for item in humantime.split():
        if (match := matcher.search(item)) is not None:
            n, unit = match.groups()
            total_ms += int(n) * unit_multiplier_map[unit]
        else:
            raise ValueError(
                f"can't parse '{item}' (from string '{humantime}'), known units are {', '.join(unit_multiplier_map.keys())}."
            )

    return round(total_ms, 3)


def scan_log_for_errors(input: Iterable[str], allowed_errors: List[str]) -> List[Tuple[int, str]]:
    # FIXME: this duplicates test_runner/fixtures/pageserver/allowed_errors.py
    error_or_warn = re.compile(r"\s(ERROR|WARN)")
    errors = []
    for lineno, line in enumerate(input, start=1):
        if len(line) == 0:
            continue

        if error_or_warn.search(line):
            # Is this a torn log line?  This happens when force-killing a process and restarting
            # Example: "2023-10-25T09:38:31.752314Z  WARN deletion executo2023-10-25T09:38:31.875947Z  INFO version: git-env:0f9452f76e8ccdfc88291bccb3f53e3016f40192"
            if re.match("\\d{4}-\\d{2}-\\d{2}T.+\\d{4}-\\d{2}-\\d{2}T.+INFO version.+", line):
                continue

            # It's an ERROR or WARN. Is it in the allow-list?
            for a in allowed_errors:
                if re.match(a, line):
                    break
            else:
                errors.append((lineno, line))
    return errors


def assert_no_errors(log_file, service, allowed_errors):
    if not log_file.exists():
        log.warning(f"Skipping {service} log check: {log_file} does not exist")
        return

    with log_file.open("r") as f:
        errors = scan_log_for_errors(f, allowed_errors)

    for _lineno, error in errors:
        log.info(f"not allowed {service} error: {error.strip()}")

    assert not errors, f"First log error on {service}: {errors[0]}\nHint: use scripts/check_allowed_errors.sh to test any new allowed_error you add"


@enum.unique
class AuxFileStore(str, enum.Enum):
    V1 = "v1"
    V2 = "v2"
    CrossValidation = "cross-validation"

    def __repr__(self) -> str:
        return f"'aux-{self.value}'"

    def __str__(self) -> str:
        return f"'aux-{self.value}'"


def assert_pageserver_backups_equal(left: Path, right: Path, skip_files: Set[str]):
    """
    This is essentially:

    lines=$(comm -3 \
        <(mkdir left && cd left && tar xf "$left" && find . -type f -print0 | xargs sha256sum | sort -k2) \
        <(mkdir right && cd right && tar xf "$right" && find . -type f -print0 | xargs sha256sum | sort -k2) \
        | wc -l)
    [ "$lines" = "0" ]

    But in a more mac friendly fashion.
    """
    started_at = time.time()

    def hash_extracted(reader: Union[IO[bytes], None]) -> bytes:
        assert reader is not None
        digest = sha256(usedforsecurity=False)
        while True:
            buf = reader.read(64 * 1024)
            if not buf:
                break
            digest.update(buf)
        return digest.digest()

    def build_hash_list(p: Path) -> List[Tuple[str, bytes]]:
        with tarfile.open(p) as f:
            matching_files = (info for info in f if info.isreg() and info.name not in skip_files)
            ret = list(
                map(lambda info: (info.name, hash_extracted(f.extractfile(info))), matching_files)
            )
            ret.sort(key=lambda t: t[0])
            return ret

    left_list, right_list = map(build_hash_list, [left, right])

    assert len(left_list) == len(
        right_list
    ), f"unexpected number of files on tar files, {len(left_list)} != {len(right_list)}"

    mismatching = set()

    for left_tuple, right_tuple in zip(left_list, right_list):
        left_path, left_hash = left_tuple
        right_path, right_hash = right_tuple
        assert (
            left_path == right_path
        ), f"file count matched, expected these to be same paths: {left_path}, {right_path}"
        if left_hash != right_hash:
            mismatching.add(left_path)

    assert len(mismatching) == 0, f"files with hash mismatch: {mismatching}"

    elapsed = time.time() - started_at
    log.info(f"assert_pageserver_backups_equal completed in {elapsed}s")


class PropagatingThread(threading.Thread):
    _target: Any
    _args: Any
    _kwargs: Any
    """
    Simple Thread wrapper with join() propagating the possible exception in the thread.
    """

    def run(self):
        self.exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super(PropagatingThread, self).join(timeout)
        if self.exc:
            raise self.exc
        return self.ret


def human_bytes(amt: float) -> str:
    """
    Render a bytes amount into nice IEC bytes string.
    """

    suffixes = ["", "Ki", "Mi", "Gi"]

    last = suffixes[-1]

    for name in suffixes:
        if amt < 1024 or name == last:
            return f"{int(round(amt))} {name}B"
        amt = amt / 1024

    raise RuntimeError("unreachable")


def all_pairs_component_versions():
    """
    This function generates all the pairs of old or new versions of the Neon components
    E.g. Pairs(storage_controller='old', storage_broker='new', compute='new', safekeeper='old', pageserver='old')
    then it returns a dictionary with argnames, argvalues and ids
    ids is a sequence of letters where n means the new version of the component and o means the old version
    """
    argvalues, ids = [], []
    all_new = False
    for pair in AllPairs(
        OrderedDict({component: ["new", "old"] for component in COMPONENT_BINARIES.keys()})
    ):
        have_old = have_new = False
        argvalues.append(pair)
        cur_id = []
        for component in COMPONENT_BINARIES.keys():
            if getattr(pair, component) == 'new':
                cur_id.append('n')
                have_new = True
            else:
                cur_id.append('o')
                have_old = True
        ids.append("combination_" + "".join(cur_id))
        all_new = all_new or (have_new and not have_old)
        assert not have_new, f"the wrong combination {pair} is generated, we don't expect only old versions"
    assert all_new, f"the combination of all-new versions was not generated, please check all-pairs setup. {argvalues}"
    return {"argnames": "combination", "argvalues": argvalues, "ids": ids}
