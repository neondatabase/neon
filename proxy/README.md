# Proxy

Proxy binary accepts `--auth-backend` CLI option, which determines auth scheme and cluster routing method. Following routing backends are currently implemented:

* console
  new SCRAM-based console API; uses SNI info to select the destination project (endpoint soon)
* postgres
  uses postgres to select auth secrets of existing roles. Useful for local testing
* link
  sends login link for all usernames

Also proxy can expose following services to the external world:

* postgres protocol over TCP -- usual postgres endpoint compatible with usual
  postgres drivers
* postgres protocol over WebSockets -- same protocol tunneled over websockets
  for environments where TCP connection is not available. We have our own
  implementation of a client that uses node-postgres and tunnels traffic through
  websockets: https://github.com/neondatabase/serverless
* SQL over HTTP -- service that accepts POST requests with SQL text over HTTP
  and responds with JSON-serialised results.


## SQL over HTTP

Contrary to the usual postgres proto over TCP and WebSockets using plain
one-shot HTTP request achieves smaller amortized latencies in edge setups due to
fewer round trips and an enhanced open connection reuse by the v8 engine. Also
such endpoint could be used directly without any driver.

To play with it locally one may start proxy over a local postgres installation
(see end of this page on how to generate certs with openssl):

```
./target/debug/proxy -c server.crt -k server.key --auth-backend=postgres --auth-endpoint=postgres://stas@127.0.0.1:5432/stas --wss 0.0.0.0:4444
```

If both postgres and proxy are running you may send a SQL query:
```console
curl -k -X POST 'https://proxy.localtest.me:4444/sql' \
  -H 'Neon-Connection-String: postgres://stas:pass@proxy.localtest.me:4444/postgres' \
  -H 'Content-Type: application/json' \
  --data '{
    "query":"SELECT $1::int[] as arr, $2::jsonb as obj, 42 as num",
    "params":[ "{{1,2},{\"3\",4}}", {"key":"val", "ikey":4242}]
  }' | jq
```
```json
{
  "command": "SELECT",
  "fields": [
    { "dataTypeID": 1007, "name": "arr" },
    { "dataTypeID": 3802, "name": "obj" },
    { "dataTypeID": 23, "name": "num" }
  ],
  "rowCount": 1,
  "rows": [
    {
      "arr": [[1,2],[3,4]],
      "num": 42,
      "obj": {
        "ikey": 4242,
        "key": "val"
      }
    }
  ]
}
```


With the current approach we made the following design decisions:

1. SQL injection protection: We employed the extended query protocol, modifying
   the rust-postgres driver to send queries in one roundtrip using a text
   protocol rather than binary, bypassing potential issues like those identified
   in sfackler/rust-postgres#1030.

2. Postgres type compatibility: As not all postgres types have binary
   representations (e.g., acl's in pg_class), we adjusted rust-postgres to
   respond with text protocol, simplifying serialization and fixing queries with
   text-only types in response.

3. Data type conversion: Considering JSON supports fewer data types than
   Postgres, we perform conversions where possible, passing all other types as
   strings. Key conversions include:
   - postgres int2, int4, float4, float8 -> json number (NaN and Inf remain
     text)
   - postgres bool, null, text -> json bool, null, string
   - postgres array -> json array
   - postgres json and jsonb -> json object

4. Alignment with node-postgres: To facilitate integration with js libraries,
   we've matched the response structure of node-postgres, returning command tags
   and column oids. Command tag capturing was added to the rust-postgres
   functionality as part of this change.

### Output options

User can pass several optional headers that will affect resulting json.

1. `Neon-Raw-Text-Output: true`. Return postgres values as text, without parsing them. So numbers, objects, booleans, nulls and arrays will be returned as text. That can be useful in cases when client code wants to implement it's own parsing or reuse parsing libraries from e.g. node-postgres.
2. `Neon-Array-Mode: true`. Return postgres rows as arrays instead of objects. That is more compact representation and also helps in some edge
cases where it is hard to use rows represented as objects (e.g. when several fields have the same name).


## Using SNI-based routing on localhost

Now proxy determines project name from the subdomain, request to the `round-rice-566201.somedomain.tld` will be routed to the project named `round-rice-566201`. Unfortunately, `/etc/hosts` does not support domain wildcards, so I usually use `*.localtest.me` which resolves to `127.0.0.1`. Now we can create self-signed certificate and play with proxy:

```sh
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.localtest.me"
```

start proxy

```sh
./target/debug/proxy -c server.crt -k server.key
```

and connect to it

```sh
PGSSLROOTCERT=./server.crt psql 'postgres://my-cluster-42.localtest.me:1234?sslmode=verify-full'
```
