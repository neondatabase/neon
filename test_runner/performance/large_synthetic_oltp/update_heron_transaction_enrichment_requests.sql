-- update approximately 4000 rows or 1 MB in the heron_transaction_enrichment_requests table
-- takes about 2 minutes
UPDATE  heron.heron_transaction_enrichment_requests  
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM  heron.heron_transaction_enrichment_requests  
    TABLESAMPLE SYSTEM (0.0002) 
);
