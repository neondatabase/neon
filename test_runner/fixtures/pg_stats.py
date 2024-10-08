from __future__ import annotations

from functools import cached_property

import pytest


class PgStatTable:
    table: str
    columns: list[str]
    additional_query: str

    def __init__(self, table: str, columns: list[str], filter_query: str = ""):
        self.table = table
        self.columns = columns
        self.additional_query = filter_query

    @cached_property
    def query(self) -> str:
        return f"SELECT {','.join(self.columns)} FROM {self.table} {self.additional_query}"


@pytest.fixture(scope="function")
def pg_stats_rw() -> list[PgStatTable]:
    return [
        PgStatTable(
            "pg_stat_database",
            ["tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted"],
            "WHERE datname='postgres'",
        ),
    ]


@pytest.fixture(scope="function")
def pg_stats_ro() -> list[PgStatTable]:
    return [
        PgStatTable(
            "pg_stat_database", ["tup_returned", "tup_fetched"], "WHERE datname='postgres'"
        ),
    ]


@pytest.fixture(scope="function")
def pg_stats_wo() -> list[PgStatTable]:
    return [
        PgStatTable(
            "pg_stat_database",
            ["tup_inserted", "tup_updated", "tup_deleted"],
            "WHERE datname='postgres'",
        ),
    ]


@pytest.fixture(scope="function")
def pg_stats_wal() -> list[PgStatTable]:
    return [
        PgStatTable(
            "pg_stat_wal",
            ["wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full", "wal_write"],
        )
    ]
