-- add 100000 rows or approximately 10 MB to the hotel_rate_mapping table
-- takes about 1 second
INSERT INTO booking_inventory.hotel_rate_mapping (
    uuid,
    created_at,
    updated_at,
    hotel_rate_id,
    remote_id,
    source
)
SELECT
    uuid_generate_v4(), -- Unique UUID for each row
    now(), -- Created at timestamp
    now(), -- Updated at timestamp
    'rate_' || gs AS hotel_rate_id, -- Unique hotel_rate_id
    'remote_' || gs AS remote_id, -- Unique remote_id
    CASE WHEN gs % 3 = 0 THEN 'source_1'
         WHEN gs % 3 = 1 THEN 'source_2'
         ELSE 'source_3'
    END AS source -- Distributing sources among three options
FROM generate_series(1, 100000) AS gs;