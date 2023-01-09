import argparse
import asyncio
import json
import logging
import signal
import sys
from typing import Any, List, Tuple

import aiohttp


class ClientException(Exception):
    pass


class Client:
    def __init__(self, pageserver_api_endpoint: str):
        self.endpoint = pageserver_api_endpoint
        self.sess = aiohttp.ClientSession()

    async def close(self):
        await self.sess.close()

    async def get_tenant_ids(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["id"] for t in body]

    async def get_timeline_ids(self, tenant_id):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant/{tenant_id}/timeline")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["timeline_id"] for t in body]

    async def timeline_spawn_download_remote_layers(self, tenant_id, timeline_id, ongoing_ok=False):

        resp = await self.sess.post(
            f"{self.endpoint}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
        )
        body = await resp.json()
        if resp.status == 409:
            if not ongoing_ok:
                raise ClientException("download already ongoing")
            pass  # response body has same shape for ongoing and newly created
        elif not resp.ok:
            raise ClientException(f"{resp}")

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
            raise ClientException(f"{resp}")

        return body


class Completed(dict[str, Any]):
    pass


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
        raise Exception(f"{tenant_id}:{timeline_id}: not starting because SIGINT received")

    # run downloads to completion

    status = await client.timeline_poll_download_remote_layers_status(tenant_id, timeline_id)
    if status is not None and status["state"] == "Running":
        raise Exception(f"{tenant_id}:{timeline_id}: download is already running")

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
    # https://www.roguelynn.com/words/asyncio-graceful-shutdowns/
    global sigint_received
    sigint_received.set()
    logging.info("SIGINT received, asyncio event set. Will not start new downloads.")


async def main(args):
    client = Client(args.pageserver_http_endpoint)

    with open(args.report_output, "w") as report_out:
        exit_code = await main_impl(args, report_out, client)

    await client.close()

    return exit_code


async def main_impl(args, report_out, client: Client):
    """
    Returns OS exit status.
    """

    tenant_and_timline_ids: List[Tuple[str, str]] = []
    # fill  tenant_and_timline_ids based on spec
    for spec in args.what:
        comps = spec.split(":")
        if comps == ["ALL"]:
            logging.info("get tenant list")
            tenant_ids = await client.get_tenant_ids()
            tasks = [
                asyncio.create_task(client.get_timeline_ids(tenant_id)) for tenant_id in tenant_ids
            ]
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            assert len(tenant_ids) == len(gathered)
            tenant_and_timline_ids = []
            for tid, tlids in zip(tenant_ids, gathered):
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

    logging.info("spawn tasks")
    concurrent_tasks_sem = asyncio.Semaphore(value=args.concurrent_tasks)

    async def task(*args):
        async with concurrent_tasks_sem:
            return await do_timeline(*args)

    tasks = [
        asyncio.create_task(task(client, tid, tlid), name=f"{tid}:{tlid}")
        for tid, tlid in tenant_and_timline_ids
    ]

    logging.info("wait for tasks to finish")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    logging.info("write report")
    report: dict[str, List[str]] = {
        "completed_without_errors": [],
        "completed_with_download_errors": [],
        "raised_exception": [],
    }
    assert len(tenant_and_timline_ids) == len(results)
    for (tid, tlid), result in zip(tenant_and_timline_ids, results):
        id = f"{tid}:{tlid}"
        logging.info(f"result for {id}: {result}")
        if isinstance(result, Completed):
            if result["failed_download_count"] == 0:
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
        required=True,
        help="where to write report output (default: stdout)",
    )
    parser.add_argument(
        "--pageserver-http-endpoint",
        required=True,
        help="where to write report output (default: stdout)",
    )
    parser.add_argument(
        "--concurrent-tasks",
        required=False,
        default=1,
        help="Max concurrent download tasks created & polled by this script",
    )
    parser.add_argument(
        "what",
        nargs="+",
        help="what to download: ALL|tenant_id|tenant_id:timeline_id",
    )
    parser.add_argument(
        "--verbose",
        type=bool,
        help="enable verbose logging",
    )
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(
        format="%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d:%H:%M:%S",
        level=level,
    )

    loop = asyncio.get_event_loop()

    loop.add_signal_handler(signal.SIGINT, handle_sigint)
    sys.exit(asyncio.run(main(args)))
