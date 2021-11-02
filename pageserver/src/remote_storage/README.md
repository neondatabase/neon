# Non-implementation details

This document describes the current state of the backup system in pageserver, existing limitations and concerns, why some things are done the way they are the future development plans.
Detailed description on how the synchronization works and how it fits into the rest of the pageserver can be found in the [storage module](./../remote_storage.rs) and its submodules.
Ideally, this document should disappear after current implementation concerns are mitigated, with the remaining useful knowledge bits moved into rustdocs.

## Approach

Backup functionality is a new component, appeared way after the core DB functionality was implemented.
Pageserver layer functionality is also quite volatile at the moment, there's a risk its local file management changes over time.

To avoid adding more chaos into that, backup functionality is currently designed as a relatively standalone component, with the majority of its logic placed in a standalone async loop.
This way, the backups are managed in background, not affecting directly other pageserver parts: this way the backup and restoration process may lag behind, but eventually keep up with the reality. To track that, a set of prometheus metrics is exposed from pageserver.

## What's done

Current implementation
* provides remote storage wrappers for AWS S3 and local FS
* uploads layers, frozen by pageserver checkpoint thread
* downloads and registers layers, found on the remote storage, but missing locally
* uses compression when deals with files, for better S3 usage
* maintains an index of what's stored remotely
* evicts failing tasks and stops the corresponding timelines

No good optimisations or performance testing is done, the feature is disabled by default and gets polished over time.
It's planned to deal with all questions that are currently on and prepare the feature to be enabled by default in cloud environments.

### Peculiarities

As mentioned, the backup component is rather new and under development currently, so not all things are done properly from the start.
Here's the list of known compromises with comments:

* Remote storage file model is currently a custom archive format, that's not possible to deserialize without a particular Rust code of ours (including `serde`).
We also don't optimize the archivation and pack every timeline checkpoint separately, so the resulting blob's size that gets on S3 could be arbitrary.
But, it's a single blob, which is way better than storing ~780 small files separately.

* Archive index restoration requires reading every blob's head.
This could be avoided by a background thread/future storing the serialized index in the remote storage.

* no proper file comparison

No file checksum assertion is done currently, but should be (AWS S3 returns file checksums during the `list` operation)

* sad rust-s3 api

rust-s3 is not very pleasant to use:
1. it returns `anyhow::Result` and it's hard to distinguish "missing file" cases from "no connection" one, for instance
2. at least one function it its API that we need (`get_object_stream`) has `async` keyword and blocks (!), see details [here](https://github.com/zenithdb/zenith/pull/752#discussion_r728373091)
3. it's a prerelease library with unclear maintenance status
4. noisy on debug level

But it's already used in the project, so for now it's reused to avoid bloating the dependency tree.
Based on previous evaluation, even `rusoto-s3` could be a better choice over this library, but needs further benchmarking.


* gc and branches are ignored

So far, we don't consider non-main images and don't adjust the remote storage based on GC thread loop results.
Only checkpointer loop affects the remote storage.

* more timelines should be downloaded on demand

Since we download and load remote layers into pageserver, there's a possibility a need for those layers' ancestors arise.
Most probably, every downloaded image's ancestor is not present in locally too, but currently there's no logic for downloading such ancestors and their metadata,
so the pageserver is unable to respond property on requests to such ancestors.

To implement the downloading, more `tenant_mgr` refactoring is needed to properly handle web requests for layers and handle the state changes.
[Here](https://github.com/zenithdb/zenith/pull/689#issuecomment-931216193) are the details about initial state management updates needed.

* no IT tests

Automated S3 testing is lacking currently, due to no convenient way to enable backups during the tests.
After it's fixed, benchmark runs should also be carried out to find bottlenecks.
