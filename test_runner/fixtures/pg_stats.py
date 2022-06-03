from typing import List

import pytest


class PgStatTable:
    table: str
    columns: List[str]
    additional_query: str

    def __init__(self, table: str, columns: List[str], filter_query: str = ""):
        self.table = table
        self.columns = columns
        self.additional_query = filter_query

    @property
    def select(self) -> str:
        return f"SELECT {','.join(self.columns)} FROM {self.table} {self.additional_query}"


# a default set of PostgreSQL statistics [1] reported when calling `PgCompare.record_pg_stats`.
# [1]: https://www.postgresql.org/docs/current/monitoring-stats.html
PG_STATS: List[PgStatTable] = [
    PgStatTable("pg_stat_database",
                ["tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted"],
                "WHERE datname='postgres'")
]


@pytest.fixture(scope='function')
def pg_stats() -> List[PgStatTable]:
    return PG_STATS
