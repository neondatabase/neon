UPDATE tenant_shards
SET config = jsonb_set(
    config::jsonb,
    '{timeline_get_throttle}',
    'null'::jsonb,
    true
);
