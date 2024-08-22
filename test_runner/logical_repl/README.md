# Logical replication tests

## Clickhouse

```bash
export BENCHMARK_CONNSTR=postgres://user:pass@ep-abc-xyz-123.us-east-2.aws.neon.build/neondb

docker compose -f clickhouse/docker-compose.yml up -d
pytest -m remote_cluster -k test_clickhouse
docker compose -f clickhouse/docker-compose.yml down
```

## Debezium

```bash
export BENCHMARK_CONNSTR=postgres://user:pass@ep-abc-xyz-123.us-east-2.aws.neon.build/neondb

docker compose -f debezium/docker-compose.yml up -d
pytest -m remote_cluster -k test_debezium
docker compose -f debezium/docker-compose.yml down

```