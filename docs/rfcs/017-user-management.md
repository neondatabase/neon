# Postgres user and database management

We've accumulated a bunch of problems with our approach to role and database management, namely:

1. we don't allow role and database creation from Postgres, and users are complaining about that
2. fine-grained role management is not possible both from Postgres and console
3. web_access and @user are different roles, which creates object access problems in some cases

Right now, we do store users and databases both in console and Postgres, and there are two main reasons for
that:

* we want to be able to authenticate users in proxy against the console without Postgres involvement. Otherwise,
malicious brute force attempts will wake up Postgres (expensive) and may exhaust the Postgres connection pool (deny of service).
* it is handy when we can render console UI without waking up compute (e.g., show database list)

Storing the same information in two systems is a form of replication. And in the current scheme
the console is primary, and Postgres catalog is a replica.

This RFC proposes to address problems 1. and 2. by making Postgres a source of truth for roles/databases and
only caching this info in the console. So using the replication analogy, now the Postgres catalog will be primary, and
the console will be a replica. Problem 3 is a bit different and could be addressed by ditching the web_access
user and using, e.g., JWT auth for the @username user so that we do not introduce a new user (JWT is needed
since we don't know users password).

This RFC doesn't talk about giving root access to the database, which is blocked by a secure runtime setup.

## Overview

* Add `/tenant/$tenant/branch/$branch/refresh_catalog` endpoint to console management API which asks `/get_catalog` and updates cached roles/databases info.
* Whenever user edits list of databases or users postgres signals `compute_ctl` to call `/<...>/refresh_catalog` in the console
* Add password strenght check in our extension

## Postgres behavior

Default user role (@username) should have `CREATE ROLE`, `CREATE DB`, and `BYPASSRLS` privileges. We expose Postgres port
to the open internet, so we need to check passwords strength. We can use the `passwordcheck` extension or do the same
from our extension.

Whenever a user edits a list of databases or users, Postgres sends SIGHUP to `compute_ctl`. `compute_ctl` should write PID to `compute_ctl.pid` file.


## Compute_ctl behavior

Upon `SIGHUP` signal `compute_ctl` should call `/tenant/$tenant/branch/$branch/refresh_catalog` to inform console about changes in the database. The console will circle back and load the data from `/get_catalog` on compute (see next section on why this approach instead of direct PUT/PATH to the console). In the case of `/refresh_catalog` failure, we should retry it N times.

Also `compute_ctl` listens for http `/get_catalog` and returns list of databases and users upon request:
```
/get_catalog: -> {
    databases: [{
        name: "db1",
        owner: "jack"
    }],
    roles: [{
        name: "jack",
        rolepassword: "SCRAM-SHA-256..."
    }]
}
```

## Console behavior

Whenever the console receives `/refresh_catalog` on the management API it goes to compute and asks for `/get_catalog`. I suggest using this way instead of accepting a list of databases/roles directly to the console endpoint for the following reasons:

* we, anyway, will need console originated call to compute's `/get_catalog` after historical branch creation
* If an intruder gains access to some other `/tenant/$tenant/.../refresh_catalog` he won't be able to change the roles list and will just force an unnecessary reload.

`/refresh_catalog` returns HTTP 200 OK on success.

We should have a button in the admin UI to manually force `/refresh_catalog` in case of data desync.

# Scalability

On my laptop, I can create 4200 roles per second. That corresponds to 363 million roles per day. So both `/get_catalog` can become expensive, and our roles database can snowball. While we can address `/get_catalog` size by catching only the latest changes (e.g., maintain the audit table and drain it by the console), it is still not nice that a single tenant can blow up a multi-tenant console database. I would instead propose to limit the number of databases and roles by some big number like 1000 and bump this limit if somebody asks for it with a legit use case. 


# QA:

- Why implement `/get_catalog` instead of sending an SQL query from the console to the compute?

- So far, we do not allow remote superuser access to Postgres, and exposing only endpoints with fixed queries beneath them reduces the attack surface.
