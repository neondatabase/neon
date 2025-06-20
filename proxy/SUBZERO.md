# Subzero

## Setup

In the root of the proxy folder, run:

Let's create self-signed certificate by running:
```sh
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.local.neon.build"
```

bring up the database using docker compose
```sh
docker compose up -f subzero/docker-compose.yml -d
```

bring up the local proxy (but disable pg_session_jwt extension installation)
```sh
cargo run --bin local_proxy -- \
  --disable_pg_session_jwt true \
  --http 0.0.0.0:7432
```

bring up the proxy (auth broker) which also handles the /rest routes handled by subzero code
```sh
cargo run --bin proxy -- \
  --is-auth-broker true \
  -c server.crt -k server.key \
  --wss 0.0.0.0:7002 \
  --http 0.0.0.0:8080 \
  --auth-backend cplane-v1
```