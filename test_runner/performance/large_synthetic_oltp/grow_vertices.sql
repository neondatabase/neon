-- add 100000 rows or approx. 18 MB to the vertices table
-- takes about 90 seconds
INSERT INTO workflows.vertices(
  uuid,
  created_at,
  condition_block_id,
  operator,
  has_been_visited,
  reference_id,
  workflow_id,
  meta_data,
  -- id,
  action_block_id
)
SELECT
  uuid_generate_v4() AS uuid,
  now() AS created_at,
  CASE WHEN (gs % 2 = 0) THEN gs % 10 ELSE NULL END AS condition_block_id, -- Every alternate row has a condition_block_id
  'operator_' || (gs % 10) AS operator, -- Cyclical operator values (e.g., operator_0, operator_1)
  false AS has_been_visited,
  'ref_' || gs AS reference_id, -- Unique reference_id for each row
  (gs % 1000) + 1 AS workflow_id, -- Random workflow_id values between 1 and 1000
  '{}'::jsonb AS meta_data, -- Empty JSON metadata
  -- gs AS id, -- default from sequence to get unique ID
  CASE WHEN (gs % 2 = 1) THEN gs ELSE NULL END AS action_block_id -- Complementary to condition_block_id
FROM generate_series(1, 100000) AS gs;