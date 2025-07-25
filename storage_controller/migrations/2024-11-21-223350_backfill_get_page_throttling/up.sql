UPDATE tenant_shards
SET config = jsonb_set(
    config::jsonb,
    '{timeline_get_throttle}',
    '{"task_kinds":["PageRequestHandler"],"initial":16500,"refill_interval":"1s","refill_amount":5500,"max":16500}'::jsonb,
    true
)
WHERE tenant_id IN (
    SELECT t.tenant_id
    FROM tenant_shards AS t, hadron_endpoints AS e, hadron_computes AS c 
    WHERE t.tenant_id = e.tenant_id 
    AND e.endpoint_id = c.endpoint_id 
    AND (c.compute_config::jsonb) ->> 'tshirt_size' = 'x-small'
);

UPDATE tenant_shards
SET config = jsonb_set(
    config::jsonb,
    '{timeline_get_throttle}',
    '{"task_kinds":["PageRequestHandler"],"initial":33000,"refill_interval":"1s","refill_amount":11000,"max":33000}'::jsonb,
    true
)
WHERE tenant_id IN (
    SELECT t.tenant_id
    FROM tenant_shards AS t, hadron_endpoints AS e, hadron_computes AS c 
    WHERE t.tenant_id = e.tenant_id 
    AND e.endpoint_id = c.endpoint_id 
    AND (c.compute_config::jsonb) ->> 'tshirt_size' = 'small'
);

UPDATE tenant_shards
SET config = jsonb_set(
    config::jsonb,
    '{timeline_get_throttle}',
    '{"task_kinds":["PageRequestHandler"],"initial":66000,"refill_interval":"1s","refill_amount":22000,"max":66000}'::jsonb,
    true
)
WHERE tenant_id IN (
    SELECT t.tenant_id
    FROM tenant_shards AS t, hadron_endpoints AS e, hadron_computes AS c 
    WHERE t.tenant_id = e.tenant_id 
    AND e.endpoint_id = c.endpoint_id 
    AND (c.compute_config::jsonb) ->> 'tshirt_size' = 'medium'
);

UPDATE tenant_shards
SET config = jsonb_set(
    config::jsonb,
    '{timeline_get_throttle}',
    '{"task_kinds":["PageRequestHandler"],"initial":132000,"refill_interval":"1s","refill_amount":44000,"max":132000}'::jsonb,
    true
)
WHERE tenant_id IN (
    SELECT t.tenant_id
    FROM tenant_shards AS t, hadron_endpoints AS e, hadron_computes AS c 
    WHERE t.tenant_id = e.tenant_id 
    AND e.endpoint_id = c.endpoint_id 
    AND (c.compute_config::jsonb) ->> 'tshirt_size' = 'large'
);
