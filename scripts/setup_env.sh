export PGSSLROOTCERT=./server.crt
export PROXY_POOL_URL='postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full'
export PROXY_PGCAT_URL='postgresql://postgres:password@pgcat.local.neon.build:4432/postgres?sslmode=verify-full'
export PROXY_PASSTHROUGH_URL='postgresql://postgres:password@comp1.local.neon.build:4432/postgres?sslmode=verify-full'
export BACKEND_URL="$PROXY_PASSTHROUGH_URL"
export RESET_PROXY_POOL_CMD="pkill -x proxy || true; sleep 1; NEON_INTERNAL_CA_FILE=/tmp/compute-ca.crt RUST_LOG=proxy LOGFMT=text cargo run -p proxy --bin proxy --features testing -- --auth-backend cplane-v1 --auth-endpoint http://127.0.0.1:3010 --tcp-pool-enabled true --tcp-pool-mode transaction --tcp-pool-max-conns-per-key 50 --tcp-pool-idle-timeout 10s --redis-auth-type plain --redis-plain 'redis://127.0.0.1:6379' -c server.crt -k server.key --endpoint-rps-limit 5000@1s --endpoint-rps-limit 5000@60s --endpoint-rps-limit 5000@600s --wake-compute-limit 5000@1s --wake-compute-limit 5000@60s --wake-compute-limit 5000@600s >/tmp/neon-proxy-bench.log 2>&1 &"
export RESET_PROXY_PASSTHROUGH_CMD="$RESET_PROXY_POOL_CMD"
export RESET_PROXY_PGCAT_CMD="pkill -x pgcat || true; sleep 1; nohup /home/charles/db_final_project/pgcat/target/debug/pgcat /home/charles/db_final_project/pgcat/pgcat.local.toml >/tmp/pgcat-bench.log 2>&1 &" 