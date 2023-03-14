#!/bin/bash

# start pg
export LD_LIBRARY_PATH=/usr/local/v15/lib
/usr/local/v15/bin/initdb -D /data/data
/usr/local/v15/bin/pg_ctl -D /data/data -l logfile start
/usr/local/v15/bin/psql postgres://neon@localhost:5432/postgres -c "alter user neon with password 'test'"

# start proxy
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.localtest.me"
/usr/local/bin/proxy --wss 127.0.0.1:7500 -c server.crt -k server.key --auth-backend=postgres --auth-endpoint=postgres://neon@localhost:5432/postgres &

# run test
cd /tmp-proxy-test
node index.js
