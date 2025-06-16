-- add 100000 rows or approximately 10 MB to the action_kwargs table
-- takes about 5 minutes
INSERT INTO workflows.action_kwargs (created_at, key, uuid, value_id, state_value_id, action_block_id)
SELECT 
    now(),  -- Using the default value for `created_at`
    'key_' || gs.id,  -- Generating a unique key based on the id
    uuid_generate_v4(),  -- Generating a new UUID for each row
    CASE WHEN gs.id % 2 = 0 THEN gs.id ELSE NULL END,  -- Setting value_id for even ids
    CASE WHEN gs.id % 2 <> 0 THEN gs.id ELSE NULL END,  -- Setting state_value_id for odd ids
    1  -- Setting action_block_id as 1 for simplicity
FROM generate_series(1, 100000) AS gs(id);