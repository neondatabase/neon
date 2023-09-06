import contextlib
import json
import os
import re
import subprocess
import tarfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, TypeVar
from urllib.parse import urlencode

import allure
from psycopg2.extensions import cursor

from fixtures.log_helper import log

if TYPE_CHECKING:
    from fixtures.neon_fixtures import PgBin
from fixtures.types import TimelineId

Fn = TypeVar("Fn", bound=Callable[..., Any])


def get_self_dir() -> Path:
    """Get the path to the directory where this script lives."""
    return Path(__file__).resolve().parent


def subprocess_capture(
    capture_dir: Path,
    cmd: List[str],
    *,
    check=False,
    echo_stderr=False,
    echo_stdout=False,
    capture_stdout=False,
    **kwargs: Any,
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
            for line in self.in_file:
                # Only bother decoding if we are going to do something more than stream to a file
                if self.echo or self.capture:
                    string = line.decode(encoding="utf-8", errors="replace")

                    if self.echo:
                        log.info(string)

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
                    **kwargs,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                stdout_handler = OutputHandler(
                    p.stdout, stdout_f, echo=echo_stdout, capture=capture_stdout
                )
                stdout_handler.start()
                stderr_handler = OutputHandler(p.stderr, stderr_f, echo=echo_stderr, capture=False)
                stderr_handler.start()

                r = p.wait()

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


def global_counter() -> int:
    """A really dumb global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter
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
def get_dir_size(path: str) -> int:
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


ATTACHMENT_NAME_REGEX: re.Pattern = re.compile(  # type: ignore[type-arg]
    r"regression\.diffs|.+\.(?:log|stderr|stdout|filediff|metrics|html)"
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
