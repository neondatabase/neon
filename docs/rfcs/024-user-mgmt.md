# Postgres user and database management

(This supersedes the previous proposal that looked too complicated and desynchronization-prone)

We've accumulated a bunch of problems with our approach to role and database management, namely:

1. we don't allow role and database creation from Postgres, and users are complaining about that
2. fine-grained role management is not possible both from Postgres and console

Right now, we do store users and databases both in console and Postgres, and there are two main reasons for
that:

* we want to be able to authenticate users in proxy against the console without Postgres' involvement. Otherwise,
malicious brute force attempts will wake up Postgres (expensive) and may exhaust the Postgres connections limit (deny of service).
* it is handy when we can render console UI without waking up compute (e.g., show database list)

This RFC doesn't talk about giving root access to the database, which is blocked by a secure runtime setup.

## Overview

* Add Postgres extension that sends an HTTP request each time transaction that modifies users/databases is about to commit.
* Add user management API to internal console API. Also, the console should put a JWT token into the compute so that it can access management API.

## Postgres behavior

The default user role (@username) should have `CREATE ROLE`, `CREATE DB`, and `BYPASSRLS` privileges. We expose the Postgres port
to the open internet, so we need to check password strength. Now console generates strong passwords, so there is no risk of having dumb passwords. With user-provided passwords, such risks exist.

Since we store passwords in the console we should also send unencrypted password when role is created/changed. Hence communication with the console must be encrypted. Postgres also supports creating roles using hashes, in that case, we will not be able to get a raw password. So I can see the following options here:
  * roles created via SQL will *not* have raw passwords in the console
  * roles created via SQL will have raw passwords in the console, except ones that were created using hashes

I'm leaning towards the second option here as it is a bit more consistent one -- if raw password storage is enabled then we store passwords in all cases where we can store them.

To send data about roles and databases from Postgres to the console we can create the following Postgres extension:

  * Intercept role/database changes in `ProcessUtility_hook`. Here we have access to the query statement with the raw password. The hook handler itself should not dial the console immediately and rather stash info in some hashmap for later use.
  * When the transaction is about to commit we execute collected role modifications (all as one -- console should either accept all or reject all, and hence API shouldn't be REST-like). If the console request fails we can roll back the transaction. This way if the transaction is committed we know for sure that console has this information. We can use `XACT_EVENT_PRE_COMMIT` and `XACT_EVENT_PARALLEL_PRE_COMMIT` for that.
  * Extension should be mindful of the fact that it is possible to create and delete roles within the transaction.
  * We also need to track who is database owner, some coding around may be needed to get the current user when the database is created.

## Console user management API

The current public API has REST API for role management. We need to have some analog for the internal API (called mgmt API in the console code). But unlike public API here we want to have an atomic way to create several roles/databases (in cases when several roles were created in the same transaction). So something like that may work:

```
curl -X PATCH /api/v1/roles_and_databases -d '
[
    {"op":"create", "type":"role", "name": "kurt", "password":"lYgT3BlbkFJ2vBZrqv"},
    {"op":"drop", "type":"role", "name": "trout"},
    {"op":"alter", "type":"role", "name": "kilgore", "password":"3BlbkFJ2vB"},
    {"op":"create", "type":"database", "name": "db2", "owner": "eliot"},
]
'
```

Makes sense not to error out on duplicated create/delete operations (see failure modes)

## Managing users from the console

Now console puts a spec file with the list of databases/roles and delta operations in all the compute pods. `compute_ctl` then picks up that file and stubbornly executes deltas and checks data in the spec file is the same as in the Postgres. This way if the user creates a role in the UI we restart compute with a new spec file and during the start databases/roles are created. So if Postgres send an HTTP call each time role is created we need to break recursion in that case. We can do that based on application_name or some GUC or user (local == no HTTP hook).

Generally, we have several options when we are creating users via console:

1. restart compute with a new spec file, execute local SQL command; cut recursion in the extension
2. "push" spec files into running compute, execute local SQL command; cut recursion in the extension
3. "push" spec files into running compute, execute local SQL command; let extension create those roles in the console
4. avoid managing roles via spec files, send SQL commands to compute; let extension create those roles in the console

The last option is the most straightforward one, but with the raw password storage opt-out, we will not have the password to establish an SQL connection. Also, we need a spec for provisioning purposes and to address potential desync (but that is quite unlikely). So I think the easiest approach would be:

1. keep role management like it is now and cut the recursion in the extension when SQL is executed by compute_ctl
2. add "push" endpoint to the compute_ctl to avoid compute restart during the `apply_config` operation -- that can be done as a follow up to avoid increasing scope too much

## Failure modes

* during role creation via SQL role was created in the console but the connection was dropped before Postgres got acknowledgment or some error happened after acknowledgment (out of disk space, deadlock, etc):

  in that case, Postgres won't have a role that exists in the console. Compute restart will heal it (due to the spec file). Also if the console allows repeated creation/deletion user can repeat the transaction.


# Scalability

On my laptop, I can create 4200 roles per second. That corresponds to 363 million roles per day. Since each role creation ends up in the console database we can add some limit to the number of roles (could be reasonably big to not run into it often -- like 1k or 10k).
