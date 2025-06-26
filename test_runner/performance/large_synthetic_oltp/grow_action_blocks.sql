-- add 100000 rows or approximately 11 MB to the action_blocks table
-- takes about 1 second
INSERT INTO workflows.action_blocks (
    id,
    uuid,
    created_at,
    status,
    function_signature,
    reference_id,
    blocking,
    run_synchronously
)
SELECT
    id,
    uuid_generate_v4(),
    now() - (random() * interval '100 days'), -- Random date within the last 100 days
    'CONDITIONS_NOT_MET',
    'function_signature_' || id, -- Create a unique function signature using id
    CASE WHEN random() > 0.5 THEN 'reference_' || id ELSE NULL END, -- 50% chance of being NULL
    true,
    CASE WHEN random() > 0.5 THEN true ELSE false END -- Random boolean value
FROM generate_series(1, 100000) AS id;