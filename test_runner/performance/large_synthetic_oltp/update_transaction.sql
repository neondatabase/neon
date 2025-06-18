-- update approximately 2000 rows or 301 MB in the transaction table
-- takes about 90 seconds
UPDATE transaction.transaction
SET is_last = NOT is_last
WHERE ctid in (
    SELECT ctid
    FROM transaction.transaction
    TABLESAMPLE SYSTEM (0.0002) 
);
