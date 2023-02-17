#
# Periodically scrape the layer maps of one or more timelines
# and store the results in an SQL database.
#

import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor
import datetime
import json
import logging
import multiprocessing
import sys
from os import getenv
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import asyncpg
import dateutil.parser
import toml


class ClientException(Exception):
    pass


class Client:
    def __init__(self, pageserver_api_endpoint: str):
        self.endpoint = pageserver_api_endpoint
        self.sess = aiohttp.ClientSession()

    async def close(self):
        await self.sess.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        await self.close()

    async def get_pageserver_id(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/status")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, dict):
            raise ClientException("expecting dict")
        return body["id"]

    async def get_tenant_ids(self):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant")
        body = await resp.json()
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["id"] for t in body]

    async def get_tenant(self, tenant_id):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant/{tenant_id}")
        body = await resp.json()
        if resp.status == 404:
            return None
        if not resp.ok:
            raise ClientException(f"{resp}")
        return body

    async def get_timeline_ids(self, tenant_id):
        resp = await self.sess.get(f"{self.endpoint}/v1/tenant/{tenant_id}/timeline")
        body = await resp.json()
        if resp.status == 404:
            return None
        if not resp.ok:
            raise ClientException(f"{resp}")
        if not isinstance(body, list):
            raise ClientException("expecting list")
        return [t["timeline_id"] for t in body]

    async def get_layer_map(
        self, tenant_id, timeline_id, reset
    ) -> Tuple[Optional[datetime.datetime], Any]:
        resp = await self.sess.get(
            f"{self.endpoint}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer",
            params={"reset": reset},
        )
        if not resp.ok:
            raise ClientException(f"{resp}")
        launch_ts_str = resp.headers["PAGESERVER_LAUNCH_TIMESTAMP"]
        launch_ts = dateutil.parser.parse(launch_ts_str)
        body = await resp.json()
        return (launch_ts, body)


async def scrape_timeline(
    ps_id: str, ps_client: Client, db: asyncpg.Pool, tenant_id, timeline_id
):
    now = datetime.datetime.now()
    launch_ts, layer_map_dump = await ps_client.get_layer_map(
        tenant_id,
        timeline_id,
        # Reset the stats on every access to get max resolution on the task kind bitmap.
        # Also, under the "every scrape does a full reset" model, it's not as urgent to
        # detect pageserver restarts in post-processing, because, to answer the question
        # "How often has the layer been accessed since its existence, across ps restarts?"
        # we can simply sum up all scrape points that we have for this layer.
        reset="AllStats",
    )
    await db.execute(
        """
            insert into scrapes (scrape_ts, pageserver_id, pageserver_launch_timestamp, tenant_id, timeline_id, layer_map_dump)
            values ($1, $2, $3, $4, $5, $6::jsonb);""",
        now,
        ps_id,
        launch_ts,
        tenant_id,
        timeline_id,
        json.dumps(layer_map_dump),
    )


async def timeline_task(
    interval,
    ps_id,
    tenant_id,
    timeline_id,
    client: Client,
    db: asyncpg.Pool,
    stop_var: asyncio.Event,
):
    """
    Task loop that is responsible for scraping one timeline
    """

    last_scrape_at = await db.fetchval("select max(scrape_ts) from scrapes where tenant_id = $1 and timeline_id = $2", tenant_id, timeline_id)
    if last_scrape_at is not None:
        logging.info(f"{tenant_id}/{timeline_id}: last scrape at: {last_scrape_at}")
        next_scrape_at = last_scrape_at + datetime.timedelta(seconds=interval)
        logging.info(f"{tenant_id}/{timeline_id}: next scrape at: {next_scrape_at}")
        now = datetime.datetime.now(tz=last_scrape_at.tzinfo)
        logging.info(f"{tenant_id}/{timeline_id}: now is: {now}")
        sleep_secs = (next_scrape_at - now).total_seconds()
        if sleep_secs < 0:
            logging.warning(f"{tenant_id}/{timeline_id}: timeline was overdue for scraping (last_scrape_at={last_scrape_at})")
        else:
            logging.info(f"{tenant_id}/{timeline_id}: sleeping remaining {sleep_secs} seconds since last scrape")
            await asyncio.sleep(sleep_secs)

    while not stop_var.is_set():
        try:
            logging.info(f"begin scraping timeline {tenant_id}/{timeline_id}")
            await scrape_timeline(ps_id, client, db, tenant_id, timeline_id)
            logging.info(f"finished scraping timeline {tenant_id}/{timeline_id}")
        except Exception:
            logging.exception(f"{tenant_id}/{timeline_id} failed, stopping scraping")
            return
        # TODO: use ticker-like construct instead of sleep()
        # TODO: bail out early if stop_var is set. That needs a select()-like statement for Python. Is there any?
        await asyncio.sleep(interval)


async def resolve_what(what: List[str], client: Client):
    """
    Resolve the list of "what" arguments on the command line to (tenant,timeline) tuples.
    Format of a `what` argument: PageserverEndpoint:TimelineSpecififer
      where
        PageserverEndpoint = http://
        TimelineSpecifier = "ALL" | TenantId | TenantId:TimelineId
    Examples:
    - `ALL`: all timelines present on the pageserver
    - `3ff96c2a04c3490285cba2019e69fb51`: all timelines of tenant `3ff96c2a04c3490285cba2019e69fb51` on the pageserver
    - `3ff96c2a04c3490285cba2019e69fb51:604094d9de4bda14dfc8da3c1a73e0e4`: timeline `604094d9de4bda14dfc8da3c1a73e0e4` of tenant `3ff96c2a04c3490285cba2019e69fb51` on the pageserver
    """
    tenant_and_timline_ids: Set[Tuple[str, str]] = set()
    # fill  tenant_and_timline_ids based on spec
    for spec in what:
        comps = spec.split(":")
        if comps == ["ALL"]:
            tenant_ids = await client.get_tenant_ids()
            tenant_infos = await asyncio.gather(
                *[client.get_tenant(tenant_id) for tenant_id in tenant_ids]
            )
            id_and_info = [
                (tid, info)
                for tid, info in zip(tenant_ids, tenant_infos)
                if info is not None and info["state"] == "Active"
            ]

            async def wrapper(tid):
                return (tid, await client.get_timeline_ids(tid))

            gathered = await asyncio.gather(*[wrapper(tid) for tid, _ in id_and_info])
            for tid, tlids in gathered:
                if tlids is None:
                    continue
                for tlid in tlids:
                    tenant_and_timline_ids.add((tid, tlid))
        elif len(comps) == 1:
            tid = comps[0]
            tlids = await client.get_timeline_ids(tid)
            if tlids is None:
                continue
            for tlid in tlids:
                tenant_and_timline_ids.add((tid, tlid))
        elif len(comps) == 2:
            tenant_and_timline_ids.add((comps[0], comps[1]))
        else:
            raise ValueError(f"invalid what-spec: {spec}")

    return tenant_and_timline_ids


async def pageserver_loop(ps_config, db: asyncpg.Pool, client: Client):
    """
    Controller loop that manages the per-timeline scrape tasks.
    """

    psid = await client.get_pageserver_id()
    scrapedb_ps_id = f"{ps_config['environment']}-{psid}"

    logging.info(f"storing results for scrapedb_ps_id={scrapedb_ps_id}")

    active_tasks_lock = asyncio.Lock()
    active_tasks: Dict[Tuple[str, str], asyncio.Event] = {}
    while True:
        try:
            desired_tasks = await resolve_what(ps_config["what"], client)
        except Exception:
            logging.exception("failed to resolve --what, sleeping then retrying")
            await asyncio.sleep(10)
            continue

        async with active_tasks_lock:
            active_task_keys = set(active_tasks.keys())

            # launch new tasks
            new_tasks = desired_tasks - active_task_keys
            logging.info(f"launching new tasks: {len(new_tasks)}")
            for tenant_id, timeline_id in new_tasks:
                logging.info(
                    f"launching scrape task for timeline {tenant_id}/{timeline_id}"
                )
                stop_var = asyncio.Event()

                assert active_tasks.get((tenant_id, timeline_id)) is None
                active_tasks[(tenant_id, timeline_id)] = stop_var

                async def task_wrapper(tenant_id, timeline_id, stop_var):
                    try:
                        await timeline_task(
                            ps_config["interval_secs"],
                            scrapedb_ps_id,
                            tenant_id,
                            timeline_id,
                            client,
                            db,
                            stop_var,
                        )
                    finally:
                        async with active_tasks_lock:
                            del active_tasks[(tenant_id, timeline_id)]

                asyncio.create_task(task_wrapper(tenant_id, timeline_id, stop_var))

            # signal tasks that aren't needed anymore to stop
            tasks_to_stop = active_task_keys - desired_tasks
            for tenant_id, timeline_id in tasks_to_stop:
                logging.info(
                    f"stopping scrape task for timeline {tenant_id}/{timeline_id}"
                )
                stop_var = active_tasks[(tenant_id, timeline_id)]
                stop_var.set()
                # the task will remove itself

        # sleep without holding the lock
        await asyncio.sleep(10)


async def pageserver_process_async(ps_config, dsn):
    async with asyncpg.create_pool(dsn, min_size=2, max_size=5) as db:
        async with Client(ps_config["endpoint"]) as client:
            return await pageserver_loop(ps_config, db, client)


def pageserver_process(ps_config, dsn):
    asyncio.run(pageserver_process_async(ps_config, dsn))


def main(args):
    scrape_config = toml.load(args.config)
    ps_configs: List[Dict[Any, Any]] = scrape_config["pageservers"]
    # global attributes inherit one level downard
    for i, ps_conf in enumerate(ps_configs):
        ps_configs[i] = scrape_config | ps_conf

    # postgres connection pool is global
    dsn = f"postgres://{args.pg_user}:{args.pg_password}@{args.pg_host}/{args.pg_database}?sslmode=require"

    pageserver_processes = []
    for ps_config in ps_configs:
        p = multiprocessing.Process(target=pageserver_process, args=(ps_config, dsn))
        p.start()
        pageserver_processes.append(p)

    for p in pageserver_processes:
        p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    def envarg(flag, envvar, **kwargs):
        parser.add_argument(
            flag, default=getenv(envvar), required=not getenv(envvar), **kwargs
        )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable verbose logging",
    )
    envarg("--pg-host", "PGHOST")
    envarg("--pg-user", "PGUSER")
    envarg("--pg-password", "PGPASSWORD")
    envarg("--pg-database", "PGDATABASE")
    parser.add_argument(
        "config",
        type=argparse.FileType(),
        help="the toml config that defines what to scrape",
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

    sys.exit(main(args))
