-- add 100000 rows or approx. 24 MB to the values table
-- takes about 126 seconds
INSERT INTO workflows.values (
    id,
    type,
    int_value,
    string_value,
    child_type,
    bool_value,
    uuid,
    numeric_value,
    workflow_id,
    jsonb_value,
    parent_value_id
)
SELECT
    gs AS id,
    'TYPE_A' AS type,
    CASE WHEN selector = 1 THEN gs ELSE NULL END AS int_value,
    CASE WHEN selector = 2 THEN 'string_value_' || gs::text ELSE NULL END AS string_value,
    'CHILD_TYPE_A' AS child_type,  -- Always non-null
    CASE WHEN selector = 3 THEN (gs % 2 = 0) ELSE NULL END AS bool_value,
    uuid_generate_v4() AS uuid,  -- Always non-null
    CASE WHEN selector = 4 THEN gs * 1.0 ELSE NULL END AS numeric_value,
    (array[1, 2, 3, 4, 5])[gs % 5 + 1] AS workflow_id,  -- Use only existing workflow IDs
    CASE WHEN selector = 5 THEN ('{"key":' || gs::text || '}')::jsonb ELSE NULL END AS jsonb_value,
    (gs % 100) + 1 AS parent_value_id  -- Always non-null
FROM
    generate_series(1, 100000) AS gs,
    (SELECT floor(random() * 5 + 1)::int AS selector) AS s;