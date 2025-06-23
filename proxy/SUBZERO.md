# Subzero

## Setup

In the root of the repo folder, run:

Let's create self-signed certificate by running:
```sh
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.local.neon.build"
```

bring up the database using docker compose
```sh
docker compose up -f proxy/subzero/docker-compose.yml -d
```

bring up the local proxy (but disable pg_session_jwt extension installation)
```sh
cargo run --bin local_proxy -- \
  --disable-pg-session-jwt \
  --http 0.0.0.0:7432
```

bring up the proxy (auth broker) which also handles the /rest routes handled by subzero code
```sh
LOGFMT=text cargo run --bin proxy -- \
  --is-auth-broker true \
  --is-rest-broker true \
   -c server.crt -k server.key \
  --wss 0.0.0.0:8080 \
  --http 0.0.0.0:7002 \
  --auth-backend cplane-v1
```

```sh
curl -k -i \
  -H "Authorization: Bearer $NEON_JWT" \
  "https://127.0.0.1:8080/rest/v1/items"
```