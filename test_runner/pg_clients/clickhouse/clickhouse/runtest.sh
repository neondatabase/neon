#!/bin/bash
set -e
export PGHOST=${NEON_HOST}
export PGUSER=${NEON_USER}
export PGDATABASE=${NEON_DATABASE}
export PGPASSWORD=${NEON_PASSWORD}
timeout_query() {
  query="$1"
  expected="$2"
  timeout=${3-60}
  COUNTER=0
  FAILED=1
  while [ "${COUNTER}" -lt "$timeout" ]; do
    OUT=$(clickhouse-client -q "${query}" 2>/dev/null | sha1sum | awk '{print $1}')
    if [ "${OUT}" = "${expected}" ]; then
      FAILED=0
      break
    fi
    sleep 1
  done
  if [ ${FAILED} = 1 ]; then
    echo Cannot get the correct data from ClickHouse
    return 1
  fi
}
/entrypoint.sh > /dev/null 2>/dev/null &
while ! [ "$(clickhouse -q "select 1")" = 1 ]; do
  sleep 1
done
psql -q -c "DROP TABLE IF EXISTS table1;
        CREATE TABLE table1 (
    id         integer primary key,
    column1    varchar(10)
);
        INSERT INTO table1
   (id, column1)
   VALUES
      (1, 'abc'),
      (2, 'def');" > /dev/null
echo 'select 1; \watch' | psql -v ON_ERROR_STOP=1 > /dev/null &
WATCH_PID=$!
echo "SET allow_experimental_database_materialized_postgresql=1;
CREATE DATABASE db1_postgres
ENGINE = MaterializedPostgreSQL('${NEON_HOST}:5432', '${NEON_DATABASE}', '${NEON_USER}', '${NEON_PASSWORD}')
SETTINGS materialized_postgresql_tables_list = 'table1';" > q.sql
clickhouse-client --queries-file q.sql
timeout_query "select * from db1_postgres.table1 order by id;" d01494c6bc02657db17a9f69988ec7ced1d7bb70
psql -q -c "INSERT INTO table1
(id, column1)
VALUES
(3, 'ghi'),
(4, 'jkl');" > /dev/null
timeout_query "select * from db1_postgres.table1 order by id;" c73f5eb0c8df02699595f68fd9820c77880a67a1
sleep 5
if kill -0 ${WATCH_PID}; then
  echo 1
else
  echo "The watcher seems to exit abnormally"
  exit 1
fi
