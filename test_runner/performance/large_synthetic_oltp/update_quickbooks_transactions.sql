-- update approximately 5000 rows or 1 MB in the quickbooks_transactions table
-- takes about 30 seconds
UPDATE   accounting.quickbooks_transactions 
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM   accounting.quickbooks_transactions 
    TABLESAMPLE SYSTEM (0.0005) 
);
