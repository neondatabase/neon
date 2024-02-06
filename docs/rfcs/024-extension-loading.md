# Supporting custom user Extensions (Dynamic Extension Loading)
Created 2023-05-03

## Motivation

There are many extensions in the PostgreSQL ecosystem, and not all extensions
are of a quality that we can confidently support them. Additionally, our
current extension inclusion mechanism has several problems because we build all
extensions into the primary Compute image: We build the extensions every time
we build the compute image regardless of whether we actually need to rebuild
the image, and the inclusion of these extensions in the image adds a hard
dependency on all supported extensions - thus increasing the image size, and
with it the time it takes to download that image - increasing first start
latency.

This RFC proposes a dynamic loading mechanism that solves most of these
problems.

## Summary

`compute_ctl` is made responsible for loading extensions on-demand into
the container's file system for dynamically loaded extensions, and will also
make sure that the extensions in `shared_preload_libraries` are downloaded
before the compute node starts.

## Components

compute_ctl, PostgreSQL, neon (extension), Compute Host Node, Extension Store

## Requirements

Compute nodes with no extra extensions should not be negatively impacted by
the existence of support for many extensions.

Installing an extension into PostgreSQL should be easy.

Non-preloaded extensions shouldn't impact startup latency.

Uninstalled extensions shouldn't impact query latency.

A small latency penalty for dynamically loaded extensions is acceptable in
the first seconds of compute startup, but not in steady-state operations.

## Proposed implementation

### On-demand, JIT-loading of extensions

Before postgres starts we download 
- control files for all extensions available to that compute node;
- all `shared_preload_libraries`;

After postgres is running, `compute_ctl` listens for requests to load files.
When PostgreSQL requests a file, `compute_ctl` downloads it.

PostgreSQL requests files in the following cases:
- When loading a preload library set in `local_preload_libraries`
- When explicitly loading a library with `LOAD`
- When creating extension with `CREATE EXTENSION` (download sql scripts, (optional) extension data files and (optional) library files)))


#### Summary

Pros:
 - Startup is only as slow as it takes to load all (shared_)preload_libraries
 - Supports BYO Extension

Cons:
 - O(sizeof(extensions)) IO requirement for loading all extensions.

### Alternative solutions

1. Allow users to add their extensions to the base image
   
   Pros:
    - Easy to deploy

   Cons:
    - Doesn't scale - first start size is dependent on image size;
    - All extensions are shared across all users: It doesn't allow users to
      bring their own restrictive-licensed extensions

2. Bring Your Own compute image
   
   Pros:
    - Still easy to deploy
    - User can bring own patched version of PostgreSQL

   Cons:
    - First start latency is O(sizeof(extensions image))
    - Warm instance pool for skipping pod schedule latency is not feasible with
      O(n) custom images
    - Support channels are difficult to manage

3. Download all user extensions in bulk on compute start
   
   Pros:
    - Easy to deploy
    - No startup latency issues for "clean" users.
    - Warm instance pool for skipping pod schedule latency is possible

   Cons:
    - Downloading all extensions in advance takes a lot of time, thus startup
      latency issues

4. Store user's extensions in persistent storage
   
   Pros:
    - Easy to deploy
    - No startup latency issues
    - Warm instance pool for skipping pod schedule latency is possible

   Cons:
    - EC2 instances have only limited number of attachments shared between EBS
      volumes, direct-attached NVMe drives, and ENIs.
    - Compute instance migration isn't trivially solved for EBS mounts (e.g.
      the device is unavailable whilst moving the mount between instances).
    - EBS can only mount on one instance at a time (except the expensive IO2
      device type).

5. Store user's extensions in network drive
   
   Pros:
    - Easy to deploy
    - Few startup latency issues
    - Warm instance pool for skipping pod schedule latency is possible

   Cons:
    - We'd need networked drives, and a lot of them, which would store many
      duplicate extensions.
    - **UNCHECKED:** Compute instance migration may not work nicely with
      networked IOs


### Idea extensions

The extension store does not have to be S3 directly, but could be a Node-local
caching service on top of S3. This would reduce the load on the network for
popular extensions.

## Extension Storage implementation

The layout of the S3 bucket is as follows:
```
5615610098 // this is an extension build number
├── v14
│   ├── extensions
│   │   ├── anon.tar.zst
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   ├── anon.tar.zst
    │   └── embedding.tar.zst
    └── ext_index.json
5615261079
├── v14
│   ├── extensions
│   │   └── anon.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── anon.tar.zst
    └── ext_index.json
5623261088
├── v14
│   ├── extensions
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── embedding.tar.zst
    └── ext_index.json
```

Note that build number cannot be part of prefix because we might need extensions
from other build numbers.

`ext_index.json` stores the control files and location of extension archives. 
It also stores a list of public extensions and a library_index

We don't need to duplicate `extension.tar.zst`` files.
We only need to upload a new one if it is updated.
(Although currently we just upload every time anyways, hopefully will change
this sometime)

*access* is controlled by spec

More specifically, here is an example ext_index.json
```
{
    "public_extensions": [
        "anon",
        "pg_buffercache"
    ],
    "library_index": {
        "anon": "anon",
        "pg_buffercache": "pg_buffercache"
        // for more complex extensions like postgis
        // we might have something like:
        // address_standardizer: postgis
        // postgis_tiger: postgis
    },
    "extension_data": {
        "pg_buffercache": {
            "control_data": {
                "pg_buffercache.control": "# pg_buffercache extension \ncomment = 'examine the shared buffer cache' \ndefault_version = '1.3' \nmodule_pathname = '$libdir/pg_buffercache' \nrelocatable = true \ntrusted=true"
            },
            "archive_path": "5670669815/v14/extensions/pg_buffercache.tar.zst"
        },
        "anon": {
            "control_data": {
                "anon.control": "# PostgreSQL Anonymizer (anon) extension \ncomment = 'Data anonymization tools' \ndefault_version = '1.1.0' \ndirectory='extension/anon' \nrelocatable = false \nrequires = 'pgcrypto' \nsuperuser = false \nmodule_pathname = '$libdir/anon' \ntrusted = true \n"
            },
            "archive_path": "5670669815/v14/extensions/anon.tar.zst"
        }
    }
}
```

### How to add new extension to the Extension Storage?

Simply upload build artifacts to the S3 bucket.
Implement a CI step for that. Splitting it from compute-node-image build.

### How do we deal with extension versions and updates?

Currently, we rebuild extensions on every compute-node-image build and store them in the <build-version> prefix.
This is needed to ensure that `/share` and `/lib` files are in sync.

For extension updates, we rely on the PostgreSQL extension versioning mechanism (sql update scripts) and extension authors to not break backwards compatibility within one major version of PostgreSQL.

### Alternatives

For extensions written on trusted languages we can also adopt
`dbdev` PostgreSQL Package Manager based on `pg_tle` by Supabase.
This will increase the amount supported extensions and decrease the amount of work required to support them.
