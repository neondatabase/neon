from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from collections import defaultdict
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from typing import Any


class ClientException(Exception):
    pass


class Client:
    def __init__(self, pageserver_api_endpoint: str, max_concurrent_layer_downloads: int):
        self.endpoint = pageserver_api_endpoint
        self.max_concurrent_layer_downloads = max_concurrent_layer_downloads
        self.sess = aiohttp.ClientSession()

    async def close(self):
        await self.sess.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        await self.close()

    async def parse_response(self, resp, expected_type):
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"Response: {resp} Body: {body}")

        if not isinstance(body, expected_type):
            raise ClientException(f"expecting {expected_type.__name__}")
        return body

    async def get_tenant_ids(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant")
        payload = await self.parse_response(resp=resp, expected_type=list)
        return [t["id"] for t in payload]

    async def get_timeline_ids(self, tenant_id):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant/{tenant_id}/timeline")
        payload = await self.parse_response(resp=resp, expected_type=list)
        return [t["timeline_id"] for t in payload]

    async def timeline_spawn_download_remote_layers(self, tenant_id, timeline_id, ongoing_ok=False):
        resp = await self.sess.post(
            f"{self.endpoint}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
            json={"max_concurrent_downloads": self.max_concurrent_layer_downloads},
        )
        body = await resp.json()
        if resp.status == 409:
            if not ongoing_ok:
                raise ClientException("download already ongoing")
            # response body has same shape for ongoing and newly created
        elif not resp.ok:
            raise ClientException(f"Response: {resp} Body: {body}")

        if not isinstance(body, dict):
            raise ClientException("expecting dict")

        return body

    async def timeline_poll_download_remote_layers_status(
        self,
        tenant_id,
        timeline_id,
    ):
        resp = await self.sess.get(
            f"{self.endpoint}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
        )
        body = await resp.json()

        if resp.status == 404:
            return None
        elif not resp.ok:
            raise ClientException(f"Response: {resp} Body: {body}")

        return body


@dataclass
class Completed:
    """The status dict returned by the API"""

    status: dict[str, Any]


sigint_received = asyncio.Event()


async def do_timeline(client: Client, tenant_id, timeline_id):
    """
    Spawn download_remote_layers task for given timeline,
    then poll until the download has reached a terminal state.

    If the terminal state is not 'Completed', the method raises an exception.
    The caller is responsible for inspecting `failed_download_count`.

    If there is already a task going on when this method is invoked,
    it raises an exception.
    """

    # Don't start new downloads if user pressed SIGINT.
    # This task will show up as "raised_exception" in the report.
    if sigint_received.is_set():
        raise Exception("not starting because SIGINT received")

    # run downloads to completion

    status = await client.timeline_poll_download_remote_layers_status(tenant_id, timeline_id)
    if status is not None and status["state"] == "Running":
        raise Exception("download is already running")

    spawned = await client.timeline_spawn_download_remote_layers(
        tenant_id, timeline_id, ongoing_ok=False
    )

    while True:
        st = await client.timeline_poll_download_remote_layers_status(tenant_id, timeline_id)
        logging.info(f"{tenant_id}:{timeline_id} state is: {st}")

        if spawned["task_id"] != st["task_id"]:
            raise ClientException("download task ids changed while polling")

        if st["state"] == "Running":
            await asyncio.sleep(10)
            continue

        if st["state"] != "Completed":
            raise ClientException(
                f"download task reached terminal state != Completed: {st['state']}"
            )

        return Completed(st)


def handle_sigint():
    logging.info("SIGINT received, asyncio event set. Will not start new downloads.")
    global sigint_received
    sigint_received.set()


async def main(args):
    async with Client(args.pageserver_http_endpoint, args.max_concurrent_layer_downloads) as client:
        exit_code = await main_impl(args, args.report_output, client)

    return exit_code


async def taskq_handler(task_q, result_q):
    while True:
        try:
            (id, fut) = task_q.get_nowait()
        except asyncio.QueueEmpty:
            logging.debug("taskq_handler observed empty task_q, returning")
            return
        logging.info(f"starting task {id}")
        try:
            res = await fut
        except Exception as e:
            res = e
        result_q.put_nowait((id, res))


async def print_progress(result_q, tasks):
    while True:
        await asyncio.sleep(10)
        logging.info(f"{result_q.qsize()} / {len(tasks)} tasks done")


async def main_impl(args, report_out, client: Client):
    """
    Returns OS exit status.
    """
    tenant_and_timline_ids: list[tuple[str, str]] = []
    # fill tenant_and_timline_ids based on spec
    for spec in args.what:
        comps = spec.split(":")
        if comps == ["ALL"]:
            logging.info("get tenant list")
            tenant_ids = await client.get_tenant_ids()
            get_timeline_id_coros = [client.get_timeline_ids(tenant_id) for tenant_id in tenant_ids]
            gathered = await asyncio.gather(*get_timeline_id_coros, return_exceptions=True)
            tenant_and_timline_ids = []
            for tid, tlids in zip(tenant_ids, gathered, strict=True):
                # TODO: add error handling if tlids isinstance(Exception)
                assert isinstance(tlids, list)

                for tlid in tlids:
                    tenant_and_timline_ids.append((tid, tlid))
        elif len(comps) == 1:
            tid = comps[0]
            tlids = await client.get_timeline_ids(tid)
            for tlid in tlids:
                tenant_and_timline_ids.append((tid, tlid))
        elif len(comps) == 2:
            tenant_and_timline_ids.append((comps[0], comps[1]))
        else:
            raise ValueError(f"invalid what-spec: {spec}")

    logging.info("expanded spec:")
    for tid, tlid in tenant_and_timline_ids:
        logging.info(f"{tid}:{tlid}")

    logging.info("remove duplicates after expanding spec")
    tmp = list(set(tenant_and_timline_ids))
    assert len(tmp) <= len(tenant_and_timline_ids)
    if len(tmp) != len(tenant_and_timline_ids):
        logging.info(f"spec had {len(tenant_and_timline_ids) - len(tmp)} duplicates")
    tenant_and_timline_ids = tmp

    logging.info("create tasks and process them at specified concurrency")
    task_q: asyncio.Queue[tuple[str, Awaitable[Any]]] = asyncio.Queue()
    tasks = {
        f"{tid}:{tlid}": do_timeline(client, tid, tlid) for tid, tlid in tenant_and_timline_ids
    }
    for task in tasks.items():
        task_q.put_nowait(task)

    result_q: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
    taskq_handlers = []
    for _ in range(0, args.concurrent_tasks):
        taskq_handlers.append(taskq_handler(task_q, result_q))

    print_progress_task = asyncio.create_task(print_progress(result_q, tasks))

    await asyncio.gather(*taskq_handlers)
    print_progress_task.cancel()

    logging.info("all tasks handled, generating report")

    results = []
    while True:
        try:
            results.append(result_q.get_nowait())
        except asyncio.QueueEmpty:
            break
    assert task_q.empty()

    report = defaultdict(list)
    for id, result in results:
        logging.info(f"result for {id}: {result}")
        if isinstance(result, Completed):
            if result.status["failed_download_count"] == 0:
                report["completed_without_errors"].append(id)
            else:
                report["completed_with_download_errors"].append(id)
        elif isinstance(result, Exception):
            report["raised_exception"].append(id)
        else:
            raise ValueError("unexpected result type")
    json.dump(report, report_out)

    logging.info("--------------------------------------------------------------------------------")

    report_success = len(report["completed_without_errors"]) == len(tenant_and_timline_ids)
    if not report_success:
        logging.error("One or more tasks encountered errors.")
    else:
        logging.info("All tasks reported success.")
    logging.info("Inspect log for details and report file for JSON summary.")

    return report_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report-output",
        type=argparse.FileType("w"),
        default="-",
        help="where to write report output (default: stdout)",
    )
    parser.add_argument(
        "--pageserver-http-endpoint",
        default="http://localhost:9898",
        help="pageserver http endpoint, (default http://localhost:9898)",
    )
    parser.add_argument(
        "--concurrent-tasks",
        required=False,
        default=5,
        type=int,
        help="Max concurrent download tasks created & polled by this script",
    )
    parser.add_argument(
        "--max-concurrent-layer-downloads",
        dest="max_concurrent_layer_downloads",
        required=False,
        default=8,
        type=int,
        help="Max concurrent download tasks spawned by pageserver. Each layer is a separate task.",
    )

    parser.add_argument(
        "what",
        nargs="+",
        help="what to download: ALL|tenant_id|tenant_id:timeline_id",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable verbose logging",
    )
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(
        format="%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d:%H:%M:%S",
        level=level,
    )

    loop = asyncio.get_event_loop()

    loop.add_signal_handler(signal.SIGINT, handle_sigint)
    sys.exit(asyncio.run(main(args)))
