-- add 100000 rows or approx. 30 MB to the device_fingerprint_event table
-- takes about 4 minutes
INSERT INTO authentication.device_fingerprint_event (
    uuid,
    created_at,
    identity_uuid,
    fingerprint_request_id,
    fingerprint_id,
    confidence_score,
    ip_address,
    url,
    client_referrer,
    last_seen_at,
    raw_fingerprint_response,
    session_uuid,
    fingerprint_response,
    browser_version,
    browser_name,
    device,
    operating_system,
    operating_system_version,
    user_agent,
    ip_address_location_city,
    ip_address_location_region,
    ip_address_location_country_code,
    ip_address_location_latitude,
    ip_address_location_longitude,
    is_incognito
)
SELECT
    gen_random_uuid(),  -- Generates a random UUID for primary key
    now() - (random() * interval '10 days'),  -- Random timestamp within the last 10 days
    gen_random_uuid(),  -- Random UUID for identity
    md5(gs::text),  -- Simulates unique fingerprint request ID using `md5` hash of series number
    md5((gs + 10000)::text),  -- Simulates unique fingerprint ID
    round(CAST(random() AS numeric), 2),  -- Generates a random score between 0 and 1, cast `random()` to numeric
    '192.168.' || (random() * 255)::int || '.' || (random() * 255)::int,  -- Random IP address
    'https://example.com/' || (gs % 1000),  -- Random URL with series number suffix
    CASE WHEN random() < 0.5 THEN NULL ELSE 'https://referrer.com/' || (gs % 100)::text END,  -- Random referrer, 50% chance of being NULL
    now() - (random() * interval '5 days'),  -- Last seen timestamp within the last 5 days
    NULL,  -- Keeping raw_fingerprint_response NULL for simplicity
    CASE WHEN random() < 0.3 THEN gen_random_uuid() ELSE NULL END,  -- Session UUID, 30% chance of NULL
    NULL,  -- Keeping fingerprint_response NULL for simplicity
    CASE WHEN random() < 0.5 THEN '93.0' ELSE '92.0' END,  -- Random browser version
    CASE WHEN random() < 0.5 THEN 'Firefox' ELSE 'Chrome' END,  -- Random browser name
    CASE WHEN random() < 0.5 THEN 'Desktop' ELSE 'Mobile' END,  -- Random device type
    'Windows',  -- Static value for operating system
    '10.0',  -- Static value for operating system version
    'Mozilla/5.0',  -- Static value for user agent
    'City ' || (gs % 1000)::text,  -- Random city name
    'Region ' || (gs % 100)::text,  -- Random region name
    'US',  -- Static country code
    random() * 180 - 90,  -- Random latitude between -90 and 90
    random() * 360 - 180,  -- Random longitude between -180 and 180
    random() < 0.1  -- 10% chance of being incognito
FROM generate_series(1, 100000) AS gs;