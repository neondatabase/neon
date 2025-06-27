-- add 100000 rows or approx.10 MB to the state_values table
-- takes about 14 seconds
INSERT INTO workflows.state_values (key, workflow_id, state_type, value_id)
SELECT 
    'key_' || gs::text,               -- Key: Generate as 'key_1', 'key_2', etc.
    (gs - 1) / 1000 + 1,              -- workflow_id: Distribute over a range (1000 workflows)
    'STATIC',                         -- state_type: Use constant 'STATIC' as defined in schema
    gs::bigint                        -- value_id: Use the same as the series value
FROM generate_series(1, 100000) AS gs; -- Generate 100,000 rows