# Neon local

Here I list some objectives to keep in mind when discussing neon-local design and a proposal that brings all components together.  Your comments on both parts are very welcome.

#### Why do we need it?
- For distribution - this easy to use binary will help us to build adoption among developers.
- For internal use - to test all components together.

In my understanding, we consider it to be just a mock-up version of neon-cloud.
> Question: How much should we care about durability and security issues for a local setup?


#### Why is it better than a simple local postgres?

- Easy one-line setup. As simple as `cargo install neon && neon start`

- Quick and cheap creation of compute nodes over the same storage.
> Question: How can we describe a use-case for this feature?

- Neon-local can work with S3 directly. 

- Push and pull images (snapshots) to remote S3 to exchange data with other users.

- Quick and cheap snapshot checkouts to switch back and forth in the database history.
> Question: Do we want it in the very first release? This feature seems quite complicated.

#### Distribution:

Ideally, just one binary that incorporates all elements we need.
> Question: Let's discuss pros and cons of having a separate package with modified PostgreSQL.

#### Components:

- **neon-CLI** - interface for end-users.  Turns commands to REST requests and handles responses to show them in a user-friendly way.  
CLI proposal is here https://github.com/neondatabase/rfcs/blob/003-laptop-cli.md/003-laptop-cli.md
WIP code is here: https://github.com/neondatabase/postgres/tree/main/pageserver/src/bin/cli

- **neon-console** - WEB UI with same functionality as CLI.
>Note: not for the first release.

- **neon-local** - entrypoint. Service that starts all other components and handles REST API requests. See REST API proposal below.
    > Idea: spawn all other components as child processes, so that we could shutdown everything by stopping neon-local.

- **neon-pageserver** - consists of a storage and WAL-replaying service (modified PG in current implementation).
> Question: Probably, for local setup we should be able to bypass page-storage and interact directly with S3 to avoid double caching in shared buffers and page-server?

WIP code is here: https://github.com/neondatabase/postgres/tree/main/pageserver/src

- **neon-S3** - stores base images of the database and WAL in S3 object storage. Import and export images from/to neon.
> Question: How should it operate in a local setup? Will we manage it ourselves or ask user to provide credentials for existing S3 object storage (i.e. minio)?
> Question: Do we use it together with local page store or they are interchangeable?

WIP code is ???

- **neon-safekeeper** - receives WAL from postgres, stores it durably, answers to Postgres that "sync" is succeed.
> Question: How should it operate in a local setup? In my understanding it should push WAL directly to S3 (if we use it) or store all data locally (if we use local page storage). The latter option seems meaningless (extra overhead and no gain), but it is still good to test the system.

WIP code is here: https://github.com/neondatabase/postgres/tree/main/src/bin/safekeeper

- **neon-computenode** - bottomless PostgreSQL, ideally upstream, but for a start - our modified version. User can quickly create and destroy them and work with it as a regular postgres database.
 
 WIP code is in main branch and here: https://github.com/neondatabase/postgres/commits/compute_node

#### REST API:

Service endpoint: `http://localhost:3000`

Resources:
- /storages - Where data lives: neon-pageserver or neon-s3
- /pgs - Postgres - neon-computenode
- /snapshots - snapshots **TODO**

>Question: Do we want to extend this API to manage neon components? I.e. start page-server, manage safekeepers and so on? Or they will be hardcoded to just start once and for all?

Methods and their mapping to CLI:

- /storages - neon-pageserver or neon-s3

CLI  | REST API
------------- | -------------
storage attach -n name --type [native\s3]  --path=[datadir\URL] | PUT  -d { "name": "name", "type": "native", "path": "/tmp" } /storages
storage detach -n name | DELETE /storages/:storage_name 
storage list | GET /storages
storage show -n name | GET /storages/:storage_name 


- /pgs - neon-computenode

CLI  | REST API
------------- | -------------
pg create -n name --s storage_name | PUT  -d { "name": "name", "storage_name": "storage_name" } /pgs
pg destroy -n name | DELETE /pgs/:pg_name 
pg start -n name --replica | POST -d {"action": "start", "is_replica":"replica"}  /pgs/:pg_name /actions
pg stop -n name | POST  -d {"action": "stop"}  /pgs/:pg_name /actions
pg promote -n name | POST  -d {"action": "promote"}  /pgs/:pg_name /actions
pg list | GET /pgs
pg show -n name | GET /pgs/:pg_name 

- /snapshots **TODO**

CLI  | REST API
------------- | -------------

