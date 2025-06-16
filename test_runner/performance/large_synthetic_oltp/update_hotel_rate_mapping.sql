-- update approximately 6000 rows or 600 kb in the  booking_inventory.hotel_rate_mapping table
-- takes about 1 second
UPDATE  booking_inventory.hotel_rate_mapping
SET created_at = now()
WHERE ctid in (
    SELECT ctid
    FROM  booking_inventory.hotel_rate_mapping
    TABLESAMPLE SYSTEM (0.0005) 
);
\sleep 59