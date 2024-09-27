from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, TypedDict, cast

import pytest
import requests
import yaml
from fixtures.log_helper import log

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv
    from pytest import MonkeyPatch

    class Metric(TypedDict):
        metric_name: str
        type: str
        help: str
        key_labels: tuple[str] | None
        values: tuple[str]
        query: Optional[str]
        query_ref: Optional[str]

    class Collector(TypedDict):
        collector_name: str
        metrics: list[Metric]

    class Query(TypedDict):
        query_name: str
        query: str


CONFIG_DIR = Path(__file__).parents[2] / "compute" / "etc"


@pytest.mark.parametrize(
    "collector_config", ["neon_collector.yml", "neon_collector_autoscaling.yml"]
)
def test_sql_exporter_metrics_smoke(neon_simple_env: NeonEnv, collector_config: Path):
    """
    This is a smoke test to ensure the metrics SQL queries sql_exporter work
    without errors.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    # Extract all the SQL queries from the sql_exporter config files, and run
    # them. We don't know what they're supposed to return, so all we can test
    # here is that they run without error.
    with open(CONFIG_DIR / collector_config, "r", encoding="utf-8") as f:
        collector = cast("Collector", yaml.safe_load(f))

        for metric in collector["metrics"]:
            query = metric.get("query")
            if query is not None:
                log.info("Checking query for metric %s in %s", metric["metric_name"], collector_config)
                endpoint.safe_psql(metric["query"])

        queries = collector.get("queries")
        if queries is not None:
            for query in queries:
                log.info("Checking query %s in %s", query["query_name"], collector_config)
                endpoint.safe_psql(query["query"])

    endpoint.stop_and_destroy()


SQL_EXPORTER = shutil.which("sql_exporter")


@pytest.mark.skipif(SQL_EXPORTER is None, reason="sql_exporter is not in the PATH")
@pytest.mark.parametrize(
    "sql_exporter_config,collector_config",
    [
        ("sql_exporter.yml", "neon_collector.yml"),
        ("sql_exporter_autoscaling.yml", "neon_collector_autoscaling.yml"),
    ],
)
def test_sql_exporter_metrics_e2e(
    neon_simple_env: NeonEnv,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    sql_exporter_config: str,
    collector_config: str,
):
    """
    This is a full E2E test of the sql_exporter setup to make sure it works
    without error.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    # Point the config at our endpoint
    new_sql_exporter_config = str(tmp_path / "sql_exporter.yml")
    with (
        open(
            CONFIG_DIR / sql_exporter_config,
            "r",
            encoding="utf-8",
        ) as i,
        open(new_sql_exporter_config, "w", encoding="utf-8") as o,
    ):
        conn_options = endpoint.conn_options()
        host: str = conn_options["host"]
        port: str = conn_options["port"]

        config: dict[Any, Any] = yaml.safe_load(i)
        config["target"][
            "data_source_name"
        ] = f"postgresql://cloud_admin@{host}:{port}/postgres?sslmode=disable&application_name=sql_exporter"
        config["collector_files"] = [str(CONFIG_DIR / collector_config)]
        yaml.dump(config, o)

    # sql_exporter will fail to launch if this environment variable exists
    monkeypatch.delenv("PGSERVICEFILE")

    sql_exporter = subprocess.Popen(
        [
            cast("str", SQL_EXPORTER),
            "-config.file",
            new_sql_exporter_config,
            "-web.listen-address",
            ":9499",
        ],
    )

    # Give time for sql_exporter to launch
    time.sleep(3)

    resp = requests.get("http://localhost:9499/metrics")
    sql_exporter.kill()
    resp.raise_for_status()

    endpoint.stop_and_destroy()
