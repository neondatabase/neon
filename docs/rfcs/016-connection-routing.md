# Dispatching a connection

For each client connection, Neon service needs to authenticate the
connection, and route it to the right PostgreSQL instance.

## Authentication

There are three different ways to authenticate:

- anonymous; no authentication needed
- PostgreSQL authentication
- github single sign-on using browser

In anonymous access, the user doesn't need to perform any
authentication at all. This can be used e.g. in interactive PostgreSQL
documentation, allowing you to run the examples very quickly. Similar
to sqlfiddle.com.

PostgreSQL authentication works the same as always. All the different
PostgreSQL authentication options like SCRAM, kerberos, etc. are
available. [1]

The third option is to authenticate with github single sign-on. When
you open the connection in psql, you get a link that you open with
your browser. Opening the link redirects you to github authentication,
and lets the connection to proceed. This is also known as "Link auth" [2].


## Routing the connection

When a client starts a connection, it needs to be routed to the
correct PostgreSQL instance. Routing can be done by the proxy, acting
as a man-in-the-middle, or the connection can be routed at the network
level based on the hostname or IP address.

Either way, Neon needs to identify which PostgreSQL instance the
connection should be routed to. If the instance is not already
running, it needs to be started. Some connections always require a new
PostgreSQL instance to be created, e.g. if you want to run a one-off
query against a particular point-in-time.

The PostgreSQL instance is identified by:
- Neon account (possibly anonymous)
- cluster (known as tenant in the storage?)
- branch or snapshot name
- timestamp (PITR)
- primary or read-replica
- one-off read replica
- one-off writeable branch

When you are using regular PostgreSQL authentication or anonymous
access, the connection URL needs to contain all the information needed
for the routing. With github single sign-on, the browser is involved
and some details - the Neon account in particular - can be deduced
from the authentication exchange.

There are three methods for identifying the PostgreSQL instance:

- Browser interaction (link auth)
- Options in the connection URL and the domain name
- A pre-defined endpoint, identified by domain name or IP address

### Link Auth

    postgres://<username>@start.neon.tech/<dbname>

This gives you a link that you open in browser. Clicking the link
performs github authentication, and the Neon account name is
provided to the proxy behind the scenes. The proxy routes the
connection to the primary PostgreSQL instance in cluster called
"main", branch "main".

Further ideas:
- You could pre-define a different target for link auth
  connections in the UI.
- You could have a drop-down in the browser, allowing you to connect
  to any cluster you want. Link Auth can be like Teleport.

### Connection URL

The connection URL looks like this:

    postgres://<username>@<cluster-id>.db.neon.tech/<dbname>

By default, this connects you to the primary PostgreSQL instance
running on the "main" branch in the named cluster [3]. However, you can
change that by specifying options in the connection URL. The following
options are supported:

| option name  | Description                                                                                       | Examples                                            |
| ---          | ---                                                                                               | ---                                                 |
| cluster      | Cluster name                                                                                      | cluster:myproject                                   |
| branch       | Branch name                                                                                       | branch:main                                         |
| timestamp    | Connect to an instance at given point-in-time.                                                    | timestamp:2022-04-08 timestamp:2022-04-08T11:42:16Z |
| lsn          | Connect to an instance at given LSN                                                               | lsn:0/12FF0420                                      |
| read-replica | Connect to a read-replica. If the parameter is 'new', a new instance is created for this session. | read-replica read-replica:new                       |

For example, to read branch 'testing' as it was on Mar 31, 2022, you could
specify a timestamp in the connection URL [4]:

    postgres://alice@cluster-1234.db.neon.tech/postgres?options=branch:testing,timestamp:2022-03-31

Connecting with cluster name and options can be disabled in the UI. If
disabled, you can only connect using a pre-defined endpoint.

### Pre-defined Endpoint

Instead of providing the cluster name, branch, and all those options
in the connection URL, you can define a named endpoint with the same
options.

In the UI, click "create endpoint". Fill in the details:

- Cluster name
- Branch
- timestamp or LSN
- is this for the primary or for a read replica
- etc.

When you click Finish, a named endpoint is created. You can now use the endpoint ID to connect:

    postgres://<username>@<endpoint-id>.endpoint.neon.tech/<dbname>


An endpoint can be assigned a static or dynamic IP address, so that
you can connect to it with clients that don't support TLS SNI. Maybe
bypass the proxy altogether, but that ought to be invisible to the
user.

You can limit the range of source IP addresses that are allowed to
connect to an endpoint. An endpoint can also be exposed in an Amazon
VPC, allowing direct connections from applications.


# Footnotes

[1] I'm not sure how feasible it is to set up configure like Kerberos
or LDAP in a cloud environment. But in principle I think we should
allow customers to have the full power of PostgreSQL, including all
authentication options. However, it's up to the customer to configure
it correctly.

[2] Link is a way to both authenticate and to route the connection

[3] This assumes that cluster-ids are globally unique, across all
Neon accounts.

[4] The syntax accepted in the connection URL is limited by libpq. The
only way to pass arbitrary options to the server (or our proxy) is
with the "options" keyword, and the options must be percent-encoded. I
think the above would work but i haven't tested it
