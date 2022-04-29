# Proxy

Proxy binary accepts `--auth-backend` CLI option, which determines auth scheme and cluster routing method. Following backends are currently implemented:

* legacy
  old method, when username ends with `@zenith` it uses md5 auth dbname as the cluster name; otherwise, it sends a login link and waits for the console to call back
* console
  new SCRAM-based console API; uses SNI info to select the destination cluster
* postgres
  uses postgres to select auth secrets of existing roles. Useful for local testing
* link
  sends login link for all usernames

## Using SNI-based routing on localhost

Now proxy determines cluster name from the subdomain, request to the `my-cluster-42.somedomain.tld` will be routed to the cluster named `my-cluster-42`. Unfortunately `/etc/hosts` does not support domain wildcards, so I usually use `*.localtest.me` which resolves to `127.0.0.1`. Now we can create self-signed certificate and play with proxy:

```
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj "/CN=*.localtest.me"

```

now you can start proxy:

```
./target/debug/proxy -c server.crt -k server.key
```

and connect to it:

```
PGSSLROOTCERT=./server.crt psql 'postgres://my-cluster-42.localtest.me:1234?sslmode=verify-full'
```
