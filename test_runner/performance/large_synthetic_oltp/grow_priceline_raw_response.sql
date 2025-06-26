-- add 100000 rows or approx. 20 MB to the priceline_raw_response table
-- takes about 20 seconds
INSERT INTO booking_inventory.priceline_raw_response (
    uuid, created_at, updated_at, url, base_url, path, method, params, request, response
)
SELECT 
    gen_random_uuid(),  -- Generate random UUIDs
    now() - (random() * interval '30 days'),  -- Random creation time within the past 30 days
    now() - (random() * interval '30 days'),  -- Random update time within the past 30 days
    'https://example.com/resource/' || gs,  -- Construct a unique URL for each row
    'https://example.com',  -- Base URL for all rows
    '/resource/' || gs,  -- Path for each row
    CASE WHEN gs % 2 = 0 THEN 'GET' ELSE 'POST' END,  -- Alternate between GET and POST methods
    'id=' || gs,  -- Simple parameter pattern for each row
    '{}'::jsonb,  -- Empty JSON object for request
    jsonb_build_object('status', 'success', 'data', gs)  -- Construct a valid JSON response
FROM 
    generate_series(1, 100000) AS gs;