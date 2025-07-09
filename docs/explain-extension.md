# Neon changes to Postgres EXPLAIN command

## Why do we need to include more information in EXPLAIN?

Neon contains two components: prefetch and LFC (local file cache) which may have critical impact on query performance.
Both are trying to solve the problem with relatively large round-trip between compute and page server (much larger than average access time for modern SSDs).
This is why Neon can provide comparable performance only if all data set is present at local node.
Certainly the fastest case of accessing data in Postgres is when it is present in Postgres cache (shared buffers).
Unfortunately size of shared buffer can not be changed on the flight: it requires Postgres restart. It is not acceptable for autoscaling.
This is why we have relatively small shared buffers and dynamically resized local file cache (LFC).
It is intended that LFC fits in memory, although it can improve performance even if data is read from local disk.
See https://neondb.slack.com/archives/C03QLRH7PPD/p1718714926044699
To minimize in-memory footprint of LFC and to improve sequential scan, LFC uses chunks which size is larger than size of Postgres page (right now chunk size is 1Mb).


LFC as any other cache is useless after cool restart. Also some data sets can not fit in local disk.
This is where another approach can help: prefetching. If we are able to predict which pages will be needed soon,
compute can send prefetch requests to page server before this page is actually requested by executor.
Prefetch is also used by Postgres (using `fadvise`), but only for vacuum and bitmap scan. Neon provides prefetch for more execution plan nodes:
sequential scan, index scan (prefetch of referenced heap pages), index-only scan (prefetch B-Tree leaves).

As far as work of prefetch and LFC may have critical impact on query performance, we need to provide this information to the users.
The most convenient and natural way is to include it in EXPLAIN. Two new keyword are added by Neon to EXPLAIN options: `prefetch` and `filecache`.

## prefetch

The following information is available about prefetch:
* `hits` - number of pages which are received from page server before actually requested by executor. Prefetch distance is controlled by `effective_io_concurrency` GUC. The larger it is, the more chances that page server will be able to complete request before it is needed. But it should not be larger than `neon.prefetch_buffer_size`.
* `misses` - number of accessed pages which were not prefetched. Prefetch is not implemented for all plan nodes. And even for those nodes for which it is implemented (i.e. sequential scan) some mispredictions are possible. Please notice that `hits + misses != accessed pages`. If prefetch request for the page was issued but not yet completed before the page is requested, then such access is not considered as prefetch hit or miss.
* `expired` - page can be updated by backend since the moment of sending prefetch request to page server. Or result of prefetch just not used because executor doesn't need this page (for example because of presence of `LIMIT` clause in the query).  In both cases such requests are considered as expired.
* `duplicates` - multiple prefetch requests for the same page. For some nodes predicting next pages is trivial, i.e. for sequential scan. But in case of index scan we need to prefetch referenced heap pages. And definitely index entries can have multiple references to the same heap page. Such non-unique prefetch requests are considered as duplicates.


## filecache

The following information is available about file cache (LFC):
* `hits` - number of accessed pages found in LFC.
* `misses` - number of accessed pages not found in LFC.

# LFC statistic

While `filecache` option of EXPLAIN command provides information about LFC usage in the particular query, there is also available global statistic about LFC usage.
It is provided by `neon` extension.

## `neon_lfc_stats` view

This view provides information as key-value pairs (so that new information can be added without changing neon extension interface).
The following keys are provided:
* `file_cache_hits` - total number of LFC hits (for all backends and queries since server startup).
* `file_cache_misses` - total number of LFC misses.
* `file_cache_used` - number chunks used in LFC
* `file_cache_writes` - number of pages written to LFC
* `file_cache_size` - current cache size in chunks (can not be larger than `neon.file_cache_size_limit`)
* `file_cache_used_pages` - number is used pages. As far as not all pages of the chunk can be filled with data it can be smaller than `file_cache_used*128` (128 is number of 8kB pages in 1MB chunk)
* `file_cache_evicted_pages` - number of pages evicted from LFC because working set doesn't fit in LFC.
* `file_cache_limit` - current limit of LFC size (in chunks)

## `local_cache` view
This view is similar with `pg_buffercache` view and contains the following columns:
```
(pageoffs int8,
relfilenode oid,
reltablespace oid,
reldatabase oid,
relforknumber int2,
relblocknumber int8,
accesscount int4)
```




