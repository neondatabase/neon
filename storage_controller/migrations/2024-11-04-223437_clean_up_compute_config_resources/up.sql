UPDATE hadron_computes
SET compute_config = jsonb_set(
    compute_config::jsonb,
    '{resources}',
    'null'::jsonb,
    true
)
WHERE compute_config::jsonb ? 'resources';
