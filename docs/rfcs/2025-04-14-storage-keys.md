
# Storage Encryption Key Management

## Summary

As a precursor to adding new encryption capabilities to Neon's storage services, this RFC proposes
mechanisms for creating and storing fine-grained encryption keys for user data in Neon.  We aim
to provide at least tenant granularity, but will use timeline granularity when it is simpler to do
so.

Out of scope:
- We describe an abstract KMS interface, but not particular platform implementations (such as how
  to authenticate with KMS).

## Terminology

_wrapped/unwrapped_: a wrapped encryption key is a key encrypted by another key.  For example, the key for
encrypting a timeline's pageserver data might be wrapped by some "root" key for the tenant's user account, stored in a KMS system.

_key hierarchy_: the relationships between keys which wrap each other. For example, a layer file key might
be wrapped by a pageserver tenant key, which is wrapped by a tenant's root key.

## Design Choices

Storage: S3 will be the store of record for wrapped keys.

Separate keys: Safekeeper and Pageserver will use independent keys.

AES256: rather than building a generic system for keys, we will assume that all the keys
we manage are AES256 keys -- this is the de-facto standard for enterprise data storage.

Per-object keys: rather than encrypting data objects (layer files and segment files) with
the tenant keys directly, they will be encrypted with separate keys.  This avoids cryptographic
safety issues from re-using the same key for large quantities of potentially repetitive plaintext.

S3 objects are self-contained: each encrypted file will have a metadata block in the file itself
storing the KMS-wrapped key to decrypt itself.

Key storage is optional at a per-tenant granularity: eventually this would be on by default, but:
- initially only some environments will have a KMS set up.
- Encryption has some overhead and it may be that some tenants don't want or need it.

## Design

### Summary of format changes

- Pageserver layer files and safekeeper segment objects are split into blocks and each
  block is encrypted by the layer key.
- Pageserver layer files and safekeeper segment objects get new metadata fields to
  store wrapped layer key and the KMS-wrapped timeline key.

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


### Pageserver Layer File Format

Encryption blocks are the minimum of unit of read. To read the part of the data within the encryption block
we must decrypt the whole block. All encryption blocks share the same layer key within the layer (is this safe?).

Image layers: each image is one encryption block.

Delta layers: for the first stage of the project, each delta is encrypted separately; in the future, we can batch
several small deltas into a single encryption block.

Indicies: each B+ tree node is an encryption block.

Layer format:

```
| Data Block | Data Block | Data Block | ... | Index Block | Index Block | Index Block | Metadata |
Data block = encrypt(data, layer_key)
Index block = encrypt(index, layer_key); index points a key to a offset of the data block inside the layer file.
Metadata = wrap(layer_key, timeline_key), wrap_kms(tenant_key), and other metadata we want to store in the future
```

Note that we generate a random layer_key for each of the layer. We store the layer key wrapped by the current
tenant key (described in later sections) and the KMS-wrapped tenant key in the layer.

If data compression is enabled, the data is compressed first before being encrypted (is this safe?)

This file format is used across both object storage and local storage. We do not decrypt when downloading
the layer file to the disk. Decryption is done when reading the layer.

### Layer File Format Migration

We record the file format for each of the layer file in both the index_part and the layer file name (suffix v2?).
The layer file format version will be passed into the layer readers. The re-keying operation (described below)
will migrate all layer files automatically to v2.

### Safekeeper Segment Format

TBD

### Pageserver Timeline Index

We will add a `created_at` for each of the layer file so that during re-keying (described in later sections)
we can determine which layer files to rewrite. We also record the offset of the metadata block so that it is
possible to obtain more information about the layer file without downloading the full layer file (i.e., the
exact timeline key being used to encrypt the layer file).

```
# LayerFileMetadata
{
  "format": 2,
  "created_at": "<time>",
  "metadata_block_offset": u64,
}
```

TODO: create an index for safekeeper so that it's faster to determine what files to re-key? Or we can scan all
files.

### Pageserver Key Cache

We have a hashmap from KMS-wrapped tenant key to plain key for each of the tenant so that we do not need to repeatly
unwrap the same key.

### Key rotation

Each tenant stores a tenant key in memory to encrypt all layer files generated across all timelines within
its active period. When the key rotation API gets called, we rotate the timeline key in memory by calling the
KMS API to generate a new key-pair, and all new layer files' layer keys will be encrypted using this key.

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

