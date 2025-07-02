-- update approximately 1000 rows or 200 kb in the manual_transaction table
-- takes about 2 seconds
UPDATE banking.manual_transaction
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM  banking.manual_transaction
    TABLESAMPLE SYSTEM (0.0005) 
);
