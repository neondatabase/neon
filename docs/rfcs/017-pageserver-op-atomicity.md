# Durability and atomicity of tenant/timeline operations

The pageserver has 8 tenant/timeline operations, listed below.  In
addition to that, data can be appended to a timeline by WAL receiver,
pages can be requested by the compute node, and tenant/timeline status
can be queries through the mgmt API. But these are the operations that
modify state in pageserver or in S3, and need to worry about crash
safety.

To make these operations atomic and recoverable, let's introduce a new
"tenant index file", called `tenant.json`. For each tenant, there is
one tenant index file, and it contains a list of all timelines for
that tenant:

{
  tenant_id: a93a94724945e95e1a0c448004ece2ec

  timelines: [
    { timeline_id: "9979cd302340a058606473912651f27f",
	  ancestor_id: ""
	  ancestor_lsn: "0/0"
    },
    { timeline_id: "f0a6f3372d273dd9ca3480d19e6b565c",
	  ancestor_id: "9979cd302340a058606473912651f27f"
	  ancestor_lsn: "1/1698C48"
	},
  ]
}

The file only contains the immutable metadata of each timeline, like
the point it was branched from. The changing parts, like
disk_consistent_lsn, are still stored in the per-timeline metadata
file.

This file allows us to resolve some ambiguous situations, like
remembering that a tenant exists when it doesn't have any timelines.
It also allows us to quickly fetch the list of all timelines of a
tenant, without having to perform S3 LIST operations.

Below is a brief description of all the pageserver tenant/timeline
operations, and how the steps of creating/deleting local files or
directories and uploads to S3 are performed. The steps are listed in
such an order that each operation can be sanely recovered or aborted,
if the pageserver crashes while the operation is being perfromed.

## Create tenant

Create an empty tenant. It doesn't have any timelines initially.

1. Create local tenant-directory with .temp extension
2. Create tenant.json file in the directory, with a special flag
   indicating that the tenant-creation is in progress
3. Rename the local tenant directory in place
4. Upload the tenant.json file to S3, without the flag
5. Update the local file, removing the flag

At pageserver startup, if we see a tenant.json file with the special
flag, check if the tenant exists in S3. If not, remove the local directory.
Otherwise remove the flag from local file.

## Create timeline

Create a timeline for a tenant, as result of running initdb.

1. create timeline directory locally, with .temp extension
2. run initdb, creating the initial set of layers
3. upload all layer files to S3
4. upload metadata file to S3
5. update tenant.json file in S3
6. Rename local directory in place

If we crash before step 5, S3 may have a timeline metadata file and some
layer files, without corresponding entry in tenant.json file. That's OK.
Whenever we see that, we can delete the leftover timeline files.

If we want to make that less scary, we could update a tenant.json file in S3
twice. First, add the new timeline ID to the file with a flag indicating
that it's being created. Do that before uploading anything else to S3. And
then in step 5, update tenant.json to indicate that the creation is complete.

## Branch timeline

Create a new timeline with an existing timeline as parent

1. create timeline directory locally, with .temp extension
2. create metadata file in the local directory
3. upload metadata file to S3
4. update tenant.json file in S3
5. Rename local directory in place

Like with Create timeline, if we crash between steps 3 and 4, we will
leave behind a timeline metadata file with no corresponding entry in
tenant.json.  That's harmless.

## Delete timeline

1. rename local timeline directory to have .temp extension
2. Update tenant.json file in S3
3. delete index file from S3
4. delete layer files from S3
5. delete local directory

Like with creation, if this is interrupted, we will leave behind
timeline files in S3 with no corresponding entry in tenant.json. If we
want to make that less scary, we can update tenant.json in step 2 with
a tombstone flag for the timeline we're removing, instead of removing
the entry for it outright.

## Delete tenant

1. rename local tenant directory to have .temp extension
2. delete tenant.json file in S3
3. delete all timeline index files from S3
4. delete all layer files from S3
5. delete local directory

Like with timeline creation, this can leave behind files with no corresponding
tenant.json file. We can make it less scary by adding tombstones.

## Attach tenant

1. create local tenant directory with .temp extension
2. Download tenant.json file
3. download index files for every timeline
4. download all layer files (in the future, skip this and download them on demand)
5. rename local tenant directory in place

## Detach tenant

1. rename local tenant directory to have .temp extension
2. delete local directory


## Load tenant

This happens automatically at pageserver startup, for every tenant that is found
in the tenants-directory. I.e. for every tenant that was attached to the pageserver
before the crash or shutdown.

1. download tenant.json file
2. for every timeline that's in remote tenant.json:
   1. download remote index file
   2. download all layer files that are missing locally (skip in future, and download on-demand)
   3. schedule upload of all files present locally, but missing remotely
   4. schedule index file upload
3. delete all locally present timeline directories that's not in tenant.json


On startup, delete everything with the .temp extension


- we could skip some of the downloads if we stored the S3 etag of the object in the local file,
  and compared that
