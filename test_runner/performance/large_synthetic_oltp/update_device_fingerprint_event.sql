-- update approximately 2000 rows or 1 MB in the device_fingerprint_event table
-- takes about 5 seconds
UPDATE authentication.device_fingerprint_event
SET is_incognito = NOT is_incognito
WHERE ctid in (
    SELECT ctid
    FROM authentication.device_fingerprint_event
    TABLESAMPLE SYSTEM (0.001) 
);
