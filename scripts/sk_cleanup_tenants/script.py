from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

import requests

level = logging.INFO
logging.basicConfig(
    format="%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
    level=level,
)

parser = argparse.ArgumentParser()
parser.add_argument("--trash-dir", required=True, type=Path)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--safekeeper-id", required=True, type=int)
parser.add_argument("--safekeeper-host", required=True, type=str)
args = parser.parse_args()

access_key = os.getenv("CONSOLE_API_TOKEN")
endpoint: str = "https://console-stage.neon.build/api"

trash_dir: Path = args.trash_dir
dry_run: bool = args.dry_run
logging.info(f"dry_run={dry_run}")
sk_id: int = args.safekeeper_id
sk_host: str = args.safekeeper_host

assert trash_dir.is_dir()

###


def console_get(rel_url):
    r = requests.get(
        f"{endpoint}{rel_url}",
        headers={
            "Authorization": f"Bearer {access_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    r.raise_for_status()
    return r


def tenant_is_deleted_in_console(tenant_id):
    r = console_get(f"/v1/admin/projects?search={tenant_id}&show_deleted=true")
    r = r.json()
    results = r["data"]
    assert len(results) == 1, f"unexpected results len: {results}"
    r = results[0]
    assert r["tenant"] == tenant_id, f"tenant id doesn't match: {r}"
    assert r["safekeepers"] is not None, f"safekeepers is None: {r}"
    assert any(sk["id"] == sk_id for sk in r["safekeepers"]), f"safekeeper id not found: {r}"
    assert "deleted" in r, f"{r}"
    return r["deleted"] is True


def call_delete_tenant_api(tenant_id):
    r = requests.delete(f"http://{sk_host}:7676/v1/tenant/{tenant_id}")
    r.raise_for_status()
    return r


def cleanup_tenant(tenant_id):
    tenant_dir = Path(f"/storage/safekeeper/data/{tenant_id}")

    if not tenant_dir.exists():
        logging.info("tenant directory doesn't exist, assuming it has been cleaned already")
        return

    if not tenant_is_deleted_in_console(tenant_id):
        logging.info("tenant is not deleted in console, skipping")
        return

    logging.info("assertions passed")

    if dry_run:
        return

    logging.info("deleting tenant")

    tenant_dir_in_trash = trash_dir / tenant_dir.relative_to("/")
    tenant_dir_in_trash.parent.mkdir(parents=True, exist_ok=True)

    assert not tenant_dir_in_trash.exists(), f"{tenant_dir_in_trash}"
    assert tenant_dir_in_trash.parent.exists(), f"{tenant_dir_in_trash}"
    # double-check
    assert tenant_dir.exists(), f"{tenant_dir}"
    assert tenant_dir.is_dir(), f"{tenant_dir}"

    logging.info(f"copying {tenant_dir} to {tenant_dir_in_trash}")
    shutil.copytree(src=tenant_dir, dst=tenant_dir_in_trash, symlinks=False, dirs_exist_ok=False)

    logging.info(f"deleting {tenant_dir}")
    call_delete_tenant_api(tenant_id)

    logging.info("tenant is now deleted, checking that it's gone")
    assert not tenant_dir.exists(), f"{tenant_dir}"


if os.path.exists("script.pid"):
    logging.info(
        f"script is already running, with pid={Path('script.pid').read_text()}. Terminate it first."
    )
    exit(1)

with open("script.pid", "w", encoding="utf-8") as f:
    f.write(str(os.getpid()))

logging.info(f"started script.py, pid={os.getpid()}")

for line in sys.stdin:
    tenant_id = line.strip()
    try:
        logging.info(f"start tenant {tenant_id}")
        cleanup_tenant(tenant_id)
        logging.info(f"done tenant {tenant_id}")
    except KeyboardInterrupt:
        print("KeyboardInterrupt exception is caught")
        break
    except:  # noqa: E722
        logging.exception(f"failed to clean up tenant {tenant_id}")

logging.info(f"finished script.py, pid={os.getpid()}")

os.remove("script.pid")
