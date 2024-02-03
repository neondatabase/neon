# Splitting cloud console

Created on 17.06.2022

## Summary

Currently we have `cloud` repository that contains code implementing public API for our clients as well as code for managing storage and internal infrastructure services. We can split everything user-related from everything storage-related to make it easier to test and maintain.

This RFC proposes to introduce a new control-plane service with HTTP API. The overall architecture will look like this:

```markup
.                    x
       external area x internal area
       (our clients) x (our services)
                     x
                     x                                                      ┌───────────────────────┐
                     x ┌───────────────┐   >    ┌─────────────────────┐     │      Storage (EC2)    │
                     x │  console db   │   >    │  control-plane db   │     │                       │
                     x └───────────────┘   >    └─────────────────────┘     │ - safekeepers         │
                     x         ▲           >               ▲                │ - pageservers         │
                     x         │           >               │                │                       │
┌──────────────────┐ x ┌───────┴───────┐   >               │                │     Dependencies      │
│    browser UI    ├──►│               │   >    ┌──────────┴──────────┐     │                       │
└──────────────────┘ x │               │   >    │                     │     │ - etcd                │
                     x │    console    ├───────►│    control-plane    ├────►│ - S3                  │
┌──────────────────┐ x │               │   >    │  (deployed in k8s)  │     │ - more?               │
│public API clients├──►│               │   >    │                     │     │                       │
└──────────────────┘ x └───────┬───────┘   >    └──────────┬──────────┘     └───────────────────────┘
                     x         │           >          ▲    │                            ▲
                     x         │           >          │    │                            │
                     x ┌───────┴───────┐   >          │    │                ┌───────────┴───────────┐
                     x │ dependencies  │   >          │    │                │                       │
                     x │- analytics    │   >          │    └───────────────►│       computes        │
                     x │- auth         │   >          │                     │   (deployed in k8s)   │
                     x │- billing      │   >          │                     │                       │
                     x └───────────────┘   >          │                     └───────────────────────┘
                     x                     >          │                                 ▲
                     x                     >    ┌─────┴───────────────┐                 │
┌──────────────────┐ x                     >    │                     │                 │
│                  │ x                     >    │        proxy        ├─────────────────┘
│     postgres     ├───────────────────────────►│  (deployed in k8s)  │
│      users       │ x                     >    │                     │
│                  │ x                     >    └─────────────────────┘
└──────────────────┘ x                     >
                                           >
                                           >
                             closed-source > open-source
                                           >
                                           >
```

Notes:

- diagram is simplified in the less-important places
- directed arrows are strict and mean that connections in the reverse direction are forbidden

This split is quite complex and this RFC proposes several smaller steps to achieve the larger goal: 

1. Start by refactoring the console code, the goal is to have console and control-plane code in the different directories without dependencies on each other.
2. Do similar refactoring for tables in the console database, remove queries selecting data from both console and control-plane; move control-plane tables to a separate database.
3. Implement control-plane HTTP API serving on a separate TCP port; make all console→control-plane calls to go through that HTTP API.
4. Move control-plane source code to the neon repo; start control-plane as a separate service.

## Motivation

These are the two most important problems we want to solve:

- Publish open-source implementation of all our cloud/storage features
- Make a unified control-plane that is used in all cloud (serverless) and local (tests) setups

Right now we have some closed-source code in the cloud repo. That code contains implementation for running Neon computes in k8s and without that code it’s impossible to automatically scale PostgreSQL computes. That means that we don’t have an open-source serverless PostgreSQL at the moment.

After splitting and open-sourcing control-plane service we will have source code and Docker images for all storage services. That control-plane service should have HTTP API for creating and managing tenants (including all our storage features), while proxy will listen for incoming connections and create computes on-demand.

Improving our test suite is an important task, but requires a lot of prerequisites and may require a separate RFC. Possible implementation of that is described in the section [Next steps](#next-steps).

Another piece of motivation can be a better involvement of storage development team into a control-plane. By splitting control-plane from the console, it can be more convenient to test and develop control-plane with paying less attention to “business” features, such as user management, billing and analytics.

For example, console currently requires authentication providers such as GitHub OAuth to work at all, as well as nodejs to be able to build it locally. It will be more convenient to build and run it locally without these requirements.

## Proposed implementation

### Current state of things

Let’s start with defining the current state of things at the moment of this proposal. We have three repositories containing source code:

- open-source `postgres` — our fork of postgres
- open-source `neon` — our main repository for storage source code
- closed-source `cloud` — mostly console backend and UI frontend

This proposal aims not to change anything at the existing code in `neon` and `postgres` repositories, but to create control-plane service and move it’s source code from `cloud` to the `neon` repository. That means that we need to split code in `cloud` repo only, and will consider only this repository for exploring its source code.

Let’s look at the miscellaneous things in the `cloud` repo which are NOT part of the console application, i.e. NOT the Go source code that is compiled to the `./console` binary. There we have:

- command-line tools, such as cloudbench, neonadmin
- markdown documentation
- cloud operations scripts (helm, terraform, ansible)
- configs and other things
- e2e python tests
- incidents playbooks
- UI frontend
- Make build scripts, code generation scripts
- database migrations
- swagger definitions

And also let’s take a look at what we have in the console source code, which is the service we’d like to split:

- API Servers
    - Public API v2
    - Management API v2
    - Public API v1
    - Admin API v1 (same port as Public API v1)
    - Management API v1
- Workers
    - Monitor Compute Activity
    - Watch Failed Operations
    - Availability Checker
    - Business Metrics Collector
- Internal Services
    - Auth Middleware, UserIsAdmin, Cookies
    - Cable Websocket Server
    - Admin Services
        - Global Settings, Operations, Pageservers, Platforms, Projects, Safekeepers, Users
    - Authenticate Proxy
    - API Keys
    - App Controller, serving UI HTML
    - Auth Controller
    - Branches
    - Projects
    - Psql Connect + Passwordless login
    - Users
    - Cloud Metrics
    - User Metrics
    - Invites
    - Pageserver/Safekeeper management
    - Operations, k8s/docker/common logic
    - Platforms, Regions
    - Project State
    - Projects Roles, SCRAM
    - Global Settings
- Other things
    - segment analytics integration
    - sentry integration
    - other common utilities packages

### Drawing the splitting line

The most challenging and the most important thing is to define the line that will split new control-plane service from the existing cloud service. If we don’t get it right, then we can end up with having a lot more issues without many benefits.

We propose to define that line as follows:

- everything user-related stays in the console service
- everything storage-related should be in the control-plane service
- something that falls in between should be decided where to go, but most likely should stay in the console service
- some similar parts should be in both services, such as admin/management/db_migrations

We call user-related all requests that can be connected to some user. The general idea is don’t have any user_id in the control-plane service and operate exclusively on tenant_id+timeline_id, the same way as existing storage services work now (compute, safekeeper, pageserver).

Storage-related things can be defined as doing any of the following:

- using k8s API
- doing requests to any of the storage services (proxy, compute, safekeeper, pageserver, etc..)
- tracking current status of tenants/timelines, managing lifetime of computes

Based on that idea, we can say that new control-plane service should have the following components:

- single HTTP API for everything
    - Create and manage tenants and timelines
    - Manage global settings and storage configuration (regions, platforms, safekeepers, pageservers)
    - Admin API for storage health inspection and debugging
- Workers
    - Monitor Compute Activity
    - Watch Failed Operations
    - Availability Checker
- Internal Services
    - Admin Services
        - Global Settings, Operations, Pageservers, Platforms, Tenants, Safekeepers
    - Authenticate Proxy
    - Branches
    - Psql Connect
    - Cloud Metrics
    - Pageserver/Safekeeper management
    - Operations, k8s/docker/common logic
    - Platforms, Regions
    - Tenant State
    - Compute Roles, SCRAM
    - Global Settings

---

And other components should probably stay in the console service:

- API Servers (no changes here)
    - Public API v2
    - Management API v2
    - Public API v1
    - Admin API v1 (same port as Public API v1)
    - Management API v1
- Workers
    - Business Metrics Collector
- Internal Services
    - Auth Middleware, UserIsAdmin, Cookies
    - Cable Websocket Server
    - Admin Services
        - Users admin stays the same
        - Other admin services can redirect requests to the control-plane
    - API Keys
    - App Controller, serving UI HTML
    - Auth Controller
    - Projects
    - User Metrics
    - Invites
    - Users
    - Passwordless login
- Other things
    - segment analytics integration
    - sentry integration
    - other common utilities packages

There are also miscellaneous things that are useful for all kinds of services. So we can say that these things can be in both services:

- markdown documentation
- e2e python tests
- make build scripts, code generation scripts
- database migrations
- swagger definitions

The single entrypoint to the storage should be control-plane API. After we define that API, we can have code-generated implementation for the client and for the server. The general idea is to move code implementing storage components from the console to the API implementation inside the new control-plane service.

After the code is moved to the new service, we can fill the created void by making API calls to the new service:

- authorization of the client
- mapping user_id + project_id to the tenant_id
- calling the control-plane API

### control-plane API

Currently we have the following projects API in the console:

```
GET /projects/{project_id}
PATCH /projects/{project_id}
POST /projects/{project_id}/branches
GET /projects/{project_id}/databases
POST /projects/{project_id}/databases
GET /projects/{project_id}/databases/{database_id}
PUT /projects/{project_id}/databases/{database_id}
DELETE /projects/{project_id}/databases/{database_id}
POST /projects/{project_id}/delete
GET /projects/{project_id}/issue_token
GET /projects/{project_id}/operations
GET /projects/{project_id}/operations/{operation_id}
POST /projects/{project_id}/query
GET /projects/{project_id}/roles
POST /projects/{project_id}/roles
GET /projects/{project_id}/roles/{role_name}
DELETE /projects/{project_id}/roles/{role_name}
POST /projects/{project_id}/roles/{role_name}/reset_password
POST /projects/{project_id}/start
POST /projects/{project_id}/stop
POST /psql_session/{psql_session_id}
```

It looks fine and we probably already have clients relying on it. So we should not change it, at least for now. But most of these endpoints (if not all) are related to storage, and it can suggest us what control-plane API should look like:

```
GET /tenants/{tenant_id}
PATCH /tenants/{tenant_id}
POST /tenants/{tenant_id}/branches
GET /tenants/{tenant_id}/databases
POST /tenants/{tenant_id}/databases
GET /tenants/{tenant_id}/databases/{database_id}
PUT /tenants/{tenant_id}/databases/{database_id}
DELETE /tenants/{tenant_id}/databases/{database_id}
POST /tenants/{tenant_id}/delete
GET /tenants/{tenant_id}/issue_token
GET /tenants/{tenant_id}/operations
GET /tenants/{tenant_id}/operations/{operation_id}
POST /tenants/{tenant_id}/query
GET /tenants/{tenant_id}/roles
POST /tenants/{tenant_id}/roles
GET /tenants/{tenant_id}/roles/{role_name}
DELETE /tenants/{tenant_id}/roles/{role_name}
POST /tenants/{tenant_id}/roles/{role_name}/reset_password
POST /tenants/{tenant_id}/start
POST /tenants/{tenant_id}/stop
POST /psql_session/{psql_session_id}
```

One of the options here is to use gRPC instead of the HTTP, which has some useful features, but there are some strong points towards using plain HTTP:

- HTTP API is easier to use for the clients
- we already have HTTP API in pageserver/safekeeper/console
- we probably want control-plane API to be similar to the console API, available in the cloud

### Getting updates from the storage

There can be some valid cases, when we would like to know what is changed in the storage. For example, console might want to know when user has queried and started compute and when compute was scaled to zero after that, to know how much user should pay for the service. Another example is to get info about reaching the disk space limits. Yet another example is to do analytics, such as how many users had at least one active project in a month.

All of the above cases can happen without using the console, just by accessing compute through the proxy.

To solve this, we can have a log of events occurring in the storage (event logs). That is very similar to operations table we have right now, the only difference is that events are immutable and we cannot change them after saving to the database. For example, we might want to have events for the following activities:

- We finished processing some HTTP API query, such as resetting the password
- We changed some state, such as started or stopped a compute
- Operation is created
- Operation is started for the first time
- Operation is failed for the first time
- Operation is finished

Once we save these events to the database, we can create HTTP API to subscribe to these events. That API can look like this:

```
GET /events/<cursor>

{
  "events": [...],
  "next_cursor": 123
}
```

It should be possible to replay event logs from some point of time, to get a state of almost anything from the storage services. That means that if we maintain some state in the control-plane database and we have a reason to have the same state in the console database, it is possible by polling events from the control-plane API and changing the state in the console database according to the events.

### Next steps

After implementing control-plane HTTP API and starting control-plane as a separate service, we might want to think of exploiting benefits of the new architecture, such as reorganizing test infrastructure. Possible options are listed in the  [Next steps](#next-steps-1).

## Non Goals

RFC doesn’t cover the actual cloud deployment scripts and schemas, such as terraform, ansible, k8s yaml’s and so on.

## Impacted components

Mostly console, but can also affect some storage service.

## Scalability

We should support starting several instances of the new control-plane service at the same time.

At the same time, it should be possible to use only single instance of control-plane, which can be useful for local tests.

## Security implications

New control-plane service is an internal service, so no external requests can reach it. But at the same time, it contains API to do absolutely anything with any of the tenants. That means that bad internal actor can potentially read and write all of the tenants. To make this safer, we can have one of these:

- Simple option is to protect all requests with a single private key, so that no one can make requests without having that one key.
- Another option is to have a separate token for every tenant and store these tokens in another secure place. This way it’s harder to access all tenants at once, because they have the different tokens.

## Alternative implementation

There was an idea to create a k8s operator for managing storage services and computes, but author of this RFC is not really familiar with it.

Regarding less alternative ideas, there are another options for the name of the new control-plane service:

- storage-ctl
- cloud
- cloud-ctl

## Pros/cons of proposed approaches (TODO)

Pros:

- All storage features are completely open-source
- Better tests coverage, less difference between cloud and local setups
- Easier to develop storage and cloud features, because there is no need to setup console for that
- Easier to deploy storage-only services to the any cloud

Cons:

- All storage features are completely open-source
- Distributed services mean more code to connect different services and potential network issues
- Console needs to have a dependency on storage API, there can be complications with developing new feature in a branch
- More code to JOIN data from different services (console and control-plane)

## Definition of Done

We have a new control-plane service running in the k8s. Source code for that control-plane service is located in the open-source neon repo.

## Next steps

After we’ve reached DoD, we can make further improvements.

First thing that can benefit from the split is local testing. The same control-plane service can implement starting computes as a local processes instead of k8s deployments. If it will also support starting pageservers/safekeepers/proxy for the local setup, then it can completely replace `./neon_local` binary, which is currently used for testing. The local testing environment can look like this:

```
┌─────────────────────┐     ┌───────────────────────┐
│                     │     │      Storage (local)  │
│  control-plane db   │     │                       │
│   (local process)   │     │ - safekeepers         │
│                     │     │ - pageservers         │
└──────────▲──────────┘     │                       │
           │                │     Dependencies      │
┌──────────┴──────────┐     │                       │
│                     │     │ - etcd                │
│    control-plane    ├────►│ - S3                  │
│   (local process)   │     │ - more?               │
│                     │     │                       │
└──────────┬──────────┘     └───────────────────────┘
       ▲   │                            ▲
       │   │                            │
       │   │                ┌───────────┴───────────┐
       │   │                │                       │
       │   └───────────────►│       computes        │
       │                    │   (local processes)   │
       │                    │                       │
┌──────┴──────────────┐     └───────────────────────┘
│                     │                 ▲
│        proxy        │                 │
│   (local process)   ├─────────────────┘
│                     │
└─────────────────────┘
```

The key thing here is that control-plane local service have the same API and almost the same implementation as the one deployed in the k8s. This allows to run the same e2e tests against both cloud and local setups.

For the python test_runner tests everything can stay mostly the same. To do that, we just need to replace `./neon_local` cli commands with API calls to the control-plane.

The benefit here will be in having fast local tests that are really close to our cloud setup. Bugs in k8s queries are still cannot be found when running computes as a local processes, but it should be really easy to start k8s locally (for example in k3s) and run the same tests with control-plane connected to the local k8s.

Talking about console and UI tests, after the split there should be a way to test these without spinning up all the storage locally. New control-plane service has a well-defined API, allowing us to mock it. This way we can create UI tests to verify the right calls are issued after specific UI interactions and verify that we render correct messages when API returns errors.