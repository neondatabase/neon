-- update approximately 1000 rows or 100 kb in the   receipt.ml_receipt_matching_log   table
-- takes about 1 seconds
UPDATE   receipt.ml_receipt_matching_log 
SET is_shadow_mode = NOT is_shadow_mode
WHERE ctid in (
    SELECT ctid
    FROM   receipt.ml_receipt_matching_log 
    TABLESAMPLE SYSTEM (0.0005) 
);
\sleep 59