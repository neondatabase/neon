# pg_clients

To run a single test locally:

```bash
export BENCHMARK_CONNSTR=postgres://user:pass@ep-abc-xyz-123.us-east-2.aws.neon.build/neondb

# will filter only tests with "serverless" in the name
./scripts/pytest -m remote_cluster -k serverless
```
