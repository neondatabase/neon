-- update approximately 2000 rows or 200 kb in the accounting_coding_body_tracking_category_selection table
-- takes about 1 second
UPDATE  accounting.accounting_coding_body_tracking_category_selection
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM  accounting.accounting_coding_body_tracking_category_selection
    TABLESAMPLE SYSTEM (0.0005) 
);
