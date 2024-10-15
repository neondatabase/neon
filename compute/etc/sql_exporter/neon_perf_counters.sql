WITH c AS (SELECT pg_catalog.jsonb_object_agg(metric, value) jb FROM neon.neon_perf_counters)

SELECT d.* FROM pg_catalog.jsonb_to_record((SELECT jb FROM c)) AS d(
  getpage_wait_seconds_count numeric,
  getpage_wait_seconds_sum numeric,
  getpage_prefetch_requests_total numeric,
  getpage_sync_requests_total numeric,
  getpage_prefetch_misses_total numeric,
  getpage_prefetch_discards_total numeric,
  pageserver_requests_sent_total numeric,
  pageserver_disconnects_total numeric,
  pageserver_send_flushes_total numeric
);
