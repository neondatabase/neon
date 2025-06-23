-- update approximately 10000 rows or 200 KB in the heron_transaction_enriched_log table
-- takes about 1 minutes
UPDATE heron.heron_transaction_enriched_log
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM heron.heron_transaction_enriched_log
    TABLESAMPLE SYSTEM (0.0005) 
);
