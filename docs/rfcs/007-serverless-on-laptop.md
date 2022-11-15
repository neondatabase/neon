How it works now
----------------

1. Create repository, start page server on it

```
$ zenith init
...
created main branch
new zenith repository was created in .zenith

$ zenith pageserver start
Starting pageserver at '127.0.0.1:64000' in .zenith
Page server started
```

2. Create a branch, and start a Postgres instance on it

```
$ zenith branch heikki main
branching at end of WAL: 0/15ECF68

$ zenith pg create heikki
Initializing Postgres on timeline 76cf9279915be7797095241638e64644...
Extracting base backup to create postgres instance: path=.zenith/pgdatadirs/pg1 port=55432

$ zenith pg start pg1
Starting postgres node at 'host=127.0.0.1 port=55432 user=heikki'
waiting for server to start.... done
server started
```


3. Connect to it and run queries

```
$ psql "dbname=postgres port=55432"
psql (14devel)
Type "help" for help.

postgres=# 
```


Proposal: Serverless on your Laptop
-----------------------------------

We've been talking about doing the "pg create" step automatically at
"pg start", to eliminate that step. What if we go further, go
serverless on your laptop, so that the workflow becomes just:

1. Create repository, start page server on it (same as before)

```
$ zenith init
...
created main branch
new zenith repository was created in .zenith

$ zenith pageserver start
Starting pageserver at '127.0.0.1:64000' in .zenith
Page server started
```

2. Create branch

```
$ zenith branch heikki main
branching at end of WAL: 0/15ECF68
```

3. Connect to it:

```
$ psql "dbname=postgres port=5432 branch=heikki"
psql (14devel)
Type "help" for help.

postgres=# 
```


The trick behind the scenes is that when you launch the page server,
it starts to listen on port 5432. When you connect to it with psql, it
looks at the 'branch' parameter that you passed in the connection
string. It automatically performs the "pg create" and "pg start" steps
for that branch, and then forwards the connection to the Postgres
instance that it launched. After you disconnect, if there are no more
active connections to the server running on the branch, it can
automatically shut it down again.

This is how serverless would work in the cloud. We can do it on your
laptop, too.
