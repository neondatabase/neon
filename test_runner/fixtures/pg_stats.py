from typing import List

class PgStat:
    table: str
    columns: List[str]
    filter_query: str

    def __init__(self, table: str, columns: List[str], filter_query: str = ""):
        self.table = table
        self.columns = columns
        self.filter_query = filter_query


# a default set of PostgreSQL statistics [1] reported when calling `PgCompare.record_pg_stats`.
# [1]: https://www.postgresql.org/docs/current/monitoring-stats.html
PG_STATS: List[PgStat] = [
    PgStat("pg_stat_database", ["tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted"])
]
