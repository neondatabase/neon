-- enforce a controlled number of getpages prefetch requests from a range of
-- 40 million first pages (320 GB) of a 500 GiB table
-- the table has 55 million pages


-- Zipfian distributions model real-world access patterns where:
--	A few values (popular IDs) are accessed frequently.
--	Many values are accessed rarely.
-- This is useful for simulating realistic workloads

\set alpha 1.2  
\set min_page 1
\set max_page 40000000 

\set zipf_random_page random_zipfian(:min_page, :max_page, :alpha)

-- Read 500 consecutive pages from a Zipfian-distributed random start page
-- This enforces PostgreSQL prefetching
WITH random_page AS (
    SELECT :zipf_random_page::int AS start_page
)
SELECT MAX(created_at)
FROM webhook.incoming_webhooks
WHERE ctid >= (SELECT format('(%s,1)', start_page)::tid FROM random_page)
AND ctid < (SELECT format('(%s,1)', start_page + 500)::tid FROM random_page);