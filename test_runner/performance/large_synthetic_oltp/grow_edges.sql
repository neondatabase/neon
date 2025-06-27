-- add 100000 rows or approximately 11 MB to the edges table
-- takes about 1 minute
INSERT INTO workflows.edges (created_at, workflow_id, uuid, from_vertex_id, to_vertex_id)
SELECT 
    now() - (random() * interval '365 days'), -- Random `created_at` timestamp in the last year
    (random() * 100)::int + 1,                -- Random `workflow_id` between 1 and 100
    uuid_generate_v4(),                       -- Generate a new UUID for each row
    (random() * 100000)::bigint + 1,           -- Random `from_vertex_id` between 1 and 100,000
    (random() * 100000)::bigint + 1           -- Random `to_vertex_id` between 1 and 100,000
FROM generate_series(1, 100000) AS gs;         -- Generate 100,000 sequential IDs