-- update approximately 5000 rows or 1 MB in the priceline_raw_response table
-- takes about 1 second
UPDATE booking_inventory.priceline_raw_response
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM booking_inventory.priceline_raw_response
    TABLESAMPLE SYSTEM (0.0005) 
);
