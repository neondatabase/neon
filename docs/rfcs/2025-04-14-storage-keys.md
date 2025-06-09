
# Storage Encryption Key Management

## Summary

As a precursor to adding new encryption capabilities to Neon's storage services, this RFC proposes
mechanisms for creating and storing fine-grained encryption keys for user data in Neon.  We aim
to provide at least tenant granularity, but will use timeline granularity when it is simpler to do
so.

Out of scope:
- We describe lifecycle of keys here but not the encryption of user data with these keys.
- We describe an abstract KMS interface, but not particular platform implementations (such as how
  to authenticate with KMS).

## Terminology

_wrapped/unwrapped_: a wrapped encryption key is a key encrypted by another key.  For example, the key for
encrypting a timeline's pageserver data might be wrapped by some "root" key for the tenant's user account, stored in a KMS system.

_key hierarchy_: the relationships between keys which wrap each other. For example, a layer file key might
be wrapped by a pageserver timeline key, which is wrapped by a tenant's root key.

## Design Choices

Storage: S3 will be the store of record for wrapped keys

Separate keys: Safekeeper and Pageserver will use independent keys.

AES256: rather than building a generic system for keys, we will assume that all the keys
we manage are AES256 keys -- this is the de-facto standard for enterprise data storage.

Per-object keys: rather than encrypting data objects (layer files and segment files) with
the tenant keys directly, they will be encrypted with separate keys.  This avoids cryptographic
safety issues from re-using the same key for large quantities of potentially repetitive plaintext.

Key storage is optional at a per-tenant granularity: eventually this would be on by default, but:
- initially only some environments will have a KMS set up.
- Encryption has some overhead and it may be that some tenants don't want or need it.

## Design

### Summary of format changes

- Pageserver layer files and safekeeper segment objects get new metadata fields to
  store wrapped key and version of the wrapping key
- Pageserver timeline index gets a new `keys` field to store wrapped timeline keys
- Safekeeper gets a new per-timeline manifest object in S3 to store wrapped timeline keys
- Pageserver timeline index gets per-layer metadata for wrapped key and wrapping version

### Summary of API changes

- Pageserver TenantConf API gets a new field for account ID
- Pageserver TenantConf API gets a new field for encryption mode
- Safekeeper timeline creation API gets a new field for account ID
- Controller, pageserver & safekeeper get a new timeline-scoped `rotate_key` API

### KMS interface

Neon will interoperate with different KMS APIs on different platforms.  We will implement a generic interface,
similar to how `remote_storage` wraps different object storage APIs:
- `generate(accountId, keyType, alias) -> (wrapped key, plaintext key)`
- `unwrap(accountId, ciphertext key) -> plaintext key`

Hereafter, when we talk about generating or unwrapping a key, this means a call into the KMS API.

The KMS deals with abstract "account IDs", which are not equal to tenant IDs and may not be
1:1 with tenants.  The account ID will be provided as part of tenant configuration, along
with a field to identify an encryption mode.

### Pageserver key storage

The wrapped pageserver timeline key will be stored in the timeline index object.  Because of
key rotation, multiple keys will be stored in an array, with each key having a counter version. 

```
"keys": [
    {
        # The key version: a new key with the next version is generated when rekeying
        "version": 1,
        # The wrapped key: this is unwrapped by a KMS API call when the key is to be used
        "wrapped": "<base64 string>",
        # The time the key was generated: this may be used to implement rekeying/key rotation
        # policies.
        "ctime": "<ISO 8601 timestamp>",
    },
    ...
]
```

Wrapped pageserver layer file keys will be stored in the `index_part` file, as part
of the layer metadata.

```
# LayerFileMetadata
{
    "key": {
        "version":
    }

}
```

To enable re-key procedure to drop deleted versions with old keys, and to avoid mistakes in index_part leading to irretreivable data loss, wrapped keys & version will also be stored
in the object store metadata of uploaded objects.

### Safekeeper key storage

All safekeeper storage is per-timeline.  The only concept of a tenant in the safekeeper
is as a namespace for timelines.

As the safekeeper doesn't currently have a flexible metadata object in remote storage,
we will add one.  This will initially contain:
- A configuration object that contains the accountId
- An array of keys idential to those used in the pageserver's index.

Because multiple safekeeper processes share the same remote storage path, we must be
sure to handle write races safely.  To avoid giving safekeepers a pageserver-like generation
concept (not to be confused with safekeeper's configuration generation), we may use
the conditional write primitive that is available on S3 and ABS, to implement a safe
read-then-write for operations such as key rotation, such that a given key version is
only ever implemented once.

### Key rotation

The process of key rotation is:
1. Load the version of the existing key
2. Generate a new key
3. Store the new key with the previous version incremented by 1
4. **Only once durably stored** use the new key for subsequent generation of object keys

This is the same for safekeepers and pageservers.

A storage controller API will be exposed for key rotation.

For the pageserver, it is very important that key rotation
operations respect generation safety rules, the same as timeline CRUD operations: i.e.
the operation is only durable once the new index_part is uploaded _and_ the generation of the tenant location updated is still the latest generation when the operation is complete.

For the safekeeper, the controller can call into one safekeeper to write the new
key to remote storage, and then when calling into the others they should just load
the key from remote storage.  Any safekeeper that is unavailable at the time of a key
rotation will need to be marked "dirty" by the controller and contacted as soon as it
comes back online (key rotation should not be failed if a single safekeeper is down).

### Re-keying

While re-keying and key-rotation are sometimes used synonymously, we distinguish them:
- Key rotation is generating a new key to use for new data
- Re-keying is rewriting existing data so that old keys are no longer used at all

Re-keying is a bulk data operation, and not fully defined in this RFC: it can be defined
quite simply as "For object in objects, if object key version is < the rekeying horizon,
then do a read/write cycle on the object using latest key".  This is a simple but potentially very
expensive operation, so we discuss efficiency here.

#### Pageserver re-key

For pageservers, occasional rekeying may be implemented efficiently if one tolerates using
the last few keys and doesn't insist on the latest, because pageservers periodically rewrite
their data for GC-compaction anyway.  Thereby an API call to re-key any data with an overly old
key would often be a no-op because all data was rewritten recently anyway.

When object versioning is enabled in storage, re-keying is not fully accomplished by just
re-writing live data: old versions would still contain user data encrypted with older keys.  To
fully re-key, an extra step is needed to purge old objects.  Ideally, we should only purge
old objects which were encrypted using old keys.  To this end, it would be useful to store
the encryption key version as metadata on objects, so that a scrub of deleted object versions
can efficiently select those objects that should be purged during re-key.

Checks on object versions should not only be on deleted objects: because pageserver can emit
"orphan" objects not referenced in the index under some circumstances, re-key must also 
check non-deleted objects.

To summarize, the pageserver re-key operation is:
- Iterate over index of layer files, select those with too-old key and rewrite them
- Iterate over all versions in object storage, select those with a too-old key version
  in their metadata and purge them (with a safety check that these are not referenced
  by the latest index).

It would be wise to combine the re-key procedure with an exhaustive read of a timeline's data,
to ensure that when testing & rolling this feature out we are not rendering anything unreadable
due to bugs in implementation.  Since we are deleting old versions in object storage, our
time travel recovery tool will not be any help if we get something wrong in this process.

#### Safekeeper re-key

Re-keying a safekeeper timeline requires an exhaustive walk of segment objects, read
metadata on each one and decide whether it requires rewrite.

Safekeeper currently keeps historic objects forever, so re-keying this data will get
more expensive as time goes on.  This would be a good time to add cleanup of old safekeeper
segments, but doing so is beyond the scope of this RFC.

### Enabling encryption for existing tenants

To enable encryption for an existing tenant, we may simply call key-rotation API (to generate a key),
and then re-key API (to rewrite existing data using this key).

## Observability

- To enable some external service to implement re-keying, we should publish metrics per-timeline
  on the age of their latest encryption key.
- Calls to KMS should be tracked with typical request rate/result/latency histograms to enable
  detection of a slow KMS server and/or errors.

## Alternatives considered

### Use same tenant key for safekeeper and pageserver

We could halve the number of keys in circulation by having the safekeeper and pageserver
share a key rather than working independently.

However, this would be substantially more complex to implement, as safekeepers and pageservers
currently share no storage, so some new communication path would be needed.  There is minimal
upside in sharing a key.

### No KMS dependency

We could choose to do all key management ourselves.  However, the industry standard approach
to enabling users of cloud SaaS software to self-manage keys is to use the KMS as the intermediary
between our system and the user's control of their key.  Although this RFC does not propose user-managed keys, we should design with this in mind.

### Do all key generation/wrapping in KMS service

We could avoid generating and wrapping/unwrapping object keys in our storage
services by delegating all responsibility for key operations to the KMS.  However,
KMS services have limited throughput and in some cases may charge per operation, so
it is useful to avoid doing KMS operations per-object, and restrict them to per-timeline
frequency.

### Per-tenant instead of per-timeline pageserver keys

For tenants with many timelines, we may reduce load on KMS service by
using per-tenant instead of per-timeline keys, so that we may do operations
such as creating a timeline without needing to do a KMS unwrap operation.

However, per-timeline key management is much simpler to implement on the safekeeper,
which currently has no concept of a tenant (other than as a namespace for timelines).
It is also slightly simpler to implement on the pageserver, as it avoids implementing
a tenant-scoped creation operation to initialize keys (instead, we may initialize keys
during timeline creation).

As a side benefit, per-timeline key management also enables implementing secure deletion in future
at a per-timeline granularity.

