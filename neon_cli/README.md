# Neon Run CLI

This python script simplifies running Neon components locally. TLDR how to run is below:

```bash
# Create data directories for safekeeper, pageserver and compute
mkdir -p tmp/sk1 tmp/ps tmp/compute

# Run opentelemetry server, because without it compute will not start
docker run -p 4318:4318 --rm otel/opentelemetry-collector:latest

# Export these in all terminals, or use direnv
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export NEON_BINARIES_DIR="$(cd .. && pwd)/target/debug"

./neon broker

./neon safekeeper -D tmp/sk1 --id 1

./neon pageserver -D tmp/ps \
  --broker-http='127.0.0.1:50051' \
  --id=123 \
  -c "pg_distrib_dir='$(cd .. && pwd)/pg_install'"

# Register new tenant with timeline in pageserver and generate spec for compute 
./neon compute init \
  --pageserver-pg=127.0.0.1:6400 \
  --pageserver-http 127.0.0.1:9898 \
  --safekeeper-pg=127.0.0.1:5454 \
  --pgbin=$(cd .. && pwd)/pg_install/v15/bin/postgres \
  --spec-path tmp/compute_spec.json

# Start compute with given spec
./neon compute start \
  --pgdata tmp/compute/pgdata \
  --pgbin=$(cd .. && pwd)/pg_install/v15/bin/postgres \
  --spec-path tmp/compute_spec.json
```

There are arguments specified in help, all other arguments 
passed after these are passed to the binary of the component.
To show help of each binary, you can use `./neon help <component>`.
To show help of this script, use `./neon [SUBCOMMAND ...] --help`, 
like `./neon compute init --help`. 