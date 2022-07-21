# What

Safe keeper needs to backup WAL to S3

S3 is used across the Neon for a variety of components and sharing code will reduce code maintanance cost 

# Why

In situations when other sources of data are compromised transaction log (WAL) produced by compute becomes the ultimate recovery solution 

see https://github.com/zenithdb/zenith/issues/1169 


# Proposal


## A common S3 integration

Given that S3 integration is common between both Safekeepers and Page Servers a common library can be built
The common requirement is an ability for asynchrounous upload of objects to a Remote Storage.

Objects can be represented by a byte stream or by a file, optional compression can be applied on either
Remote storage shuold have production implementation and test implementations. Production impementation is built to work with S3 API compatible blob storage while test implementation is based on a filesystem

 Commmon operations to be supported:
  1. Upload object (inputs: an object and a destination)
  2. List objects (inputs: an object filter)  
  *this is irrelevant to Safekeepers and no discussed here*
  3. Download object (inputs: a destination)  
  *this is irrelevant to Safekeepers and no discussed here*

In order to isolate tenants interface when created need to require a timeline.
This should minimize chances of object clobbering by others.

## Safe keeper specific

**WAL Backup** is a layer above basic s3 integration should allow uploading objects and perform maintain **WAL Backup LSN** metadata, it's a glue between "Storage" object and S3 infra.

Storage object would provide a notification interface to which WAL Backup would subscribe. Notifications are fired when segment is full (e.g. no more data fits into the segment and ".partial" suffix is removed).
WAL Backup will subscribe to the notification and will upload a segment, upon completion it will attempt to advance WAL Backup LSN in etcd.

WAL backup will only be enable on a single safe keeper for a timeline (election should happen via etcd vote)


## Stages

 1. WAL backup happens synchonously, LSN is advanced each time WAL segment is uploaded
 2. Crash retry support, in certain cases when safekeeper restarts we may be interrupted in the middle of upload.
    no notifications will be fired for such wal segment. Upon restart or leader re-election a catch-up operation whould enqueue complete segments beyond WAL Backup LSN 
 3. WAL backup is uploaded async (and potentially out of order) upon completion results are posted into a collection of ranges, WAL Backup LSN is advanced to the highest LSN for which all segments are uploaded.
  if WAL backup LSN can not be advanced for X number of segments or Y amount of time - an Alarm is configured to bring operator attention.

Each WAL upload task is an endless retry loop attempting to upload a segment; then attempt fails task is retried.
 following are considered terminal
 1. a segment is uploaded - this is an succesfull completion, update stats
 2. Object already exists in the backup - this is success case as well, update stats.
 3. Leader had been re-elected - task is terminated.
 4. SK shutdown is requested - task is terminated.
 5. Segment file is not found - task is terminated, Alarm.
 6. All S3 Access errors are retried, Error rate is maintained, if S3 Error rate is over threshold we should alarm.



## Details

[![](https://mermaid.ink/img/pako:eNqllMFu2zAMhl-F8CnBHAxt0MN8KLBkzQ4t2iA69LAMgWLTqVBZ8iR5mVH03UdJsZ0ly2GYD4YhfhLJn7_8luS6wCRLLP5oUOX4RfCd4dVaAT01N07koubKwRy4hW9zXdWNw-_ncebjjJd4j1ijOQeWgVi-tFbkXAJz2vAdnnMLjy2ERGCtdfiXUmaeeKYzZjx_bepzYBVSrbDSDodEkZvD5Pb2A5WbwfPnB-B1jarYGN--dRFhEVmyDPZGONzsucxgVGubwrYpSzRjgIguAwuLjuRSjg5IBBYh7o96eu1K0LXrt1OUNpe2Vflhjco5BhnuKqSehIWykbLfF0qcZfConSjbHvMDkkhtj0qSUPEKU7COtAFpVeoP9x_jLoPB3IHZbUdXn65SuL6e0uvmZjykf1KyBa0AJZFYALuPsRn4CnwBd4qka7CvYKFNP5ie9cWuSITt0cSk1jU0ygkJtslztDau-4cmGM5n0wzIcEOATWNgFRT9SD5pDB4Jd7R54qvzDG-kGyrxqyvMtSnggT2C4apzYahzAujyIqN2XDBIbMWjJ1n-_Di2wwQO4-7NNIF5Z4BB2k44DpWwVqgd2CjhmQuFEm5jo49H4yHViQcU7sGJCqVQNP9C-OFq047_f9QkeFTl6wVV_H5RBod2XVi6kvaUBvwl_DX7FxN5skCeO_GTk7Fn3e4Lk7gwhxBL0qRCU3FR0C_vzQfWiXvBCtdJRp8FlsEryVq9E9rUBSW8KwSpmGQllxbThDdOM7qt_UKkDv_Nw-r7b0DLoqo)](https://mermaid.live/edit#pako:eNqllMFu2zAMhl-F8CnBHAxt0MN8KLBkzQ4t2iA69LAMgWLTqVBZ8iR5mVH03UdJsZ0ly2GYD4YhfhLJn7_8luS6wCRLLP5oUOX4RfCd4dVaAT01N07koubKwRy4hW9zXdWNw-_ncebjjJd4j1ijOQeWgVi-tFbkXAJz2vAdnnMLjy2ERGCtdfiXUmaeeKYzZjx_bepzYBVSrbDSDodEkZvD5Pb2A5WbwfPnB-B1jarYGN--dRFhEVmyDPZGONzsucxgVGubwrYpSzRjgIguAwuLjuRSjg5IBBYh7o96eu1K0LXrt1OUNpe2Vflhjco5BhnuKqSehIWykbLfF0qcZfConSjbHvMDkkhtj0qSUPEKU7COtAFpVeoP9x_jLoPB3IHZbUdXn65SuL6e0uvmZjykf1KyBa0AJZFYALuPsRn4CnwBd4qka7CvYKFNP5ie9cWuSITt0cSk1jU0ygkJtslztDau-4cmGM5n0wzIcEOATWNgFRT9SD5pDB4Jd7R54qvzDG-kGyrxqyvMtSnggT2C4apzYahzAujyIqN2XDBIbMWjJ1n-_Di2wwQO4-7NNIF5Z4BB2k44DpWwVqgd2CjhmQuFEm5jo49H4yHViQcU7sGJCqVQNP9C-OFq047_f9QkeFTl6wVV_H5RBod2XVi6kvaUBvwl_DX7FxN5skCeO_GTk7Fn3e4Lk7gwhxBL0qRCU3FR0C_vzQfWiXvBCtdJRp8FlsEryVq9E9rUBSW8KwSpmGQllxbThDdOM7qt_UKkDv_Nw-r7b0DLoqo)

Async operations are noted in the dashed lines

* WAL Backup is a singleton created for the SK lifetime
* Notification mechanism would allow decoupling Physical Storage with WAL backup
* Safekeeper Local WAL GC should be able to subscribe to notifications from Physical storage as well
* Error handling is omitted for simplicity

## Configuration

* Wal Backup gets a bucket and prefix from SafeKeeper configuration (SK should run under IAM role that limits writes to S3 the flat wal bucket only)
* The error thresholds come from the configuration
*

# Misc

Safe keeper garbage collection should take WAL Backup LSN into account and only remove segment that have been succesfully uploaded to S3.

The S3 integration library should hide which specific client is used (s3-rust vs aws rust SDK ec) for the operation.
S3 Client configuration should be handled by the "Remote Storage" implementation (e.g. multi-part uploads, auth etc.)
