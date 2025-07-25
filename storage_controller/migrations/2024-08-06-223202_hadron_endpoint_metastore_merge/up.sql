-- Migration that makes Hadron endpoints to be per-tenant instead of per-timeline.
-- The tenant ID of the endpoint is now directly stored as a column in the `hadron_endpoints` table,
-- removing the indirection of the `hadron_metastores` table.
ALTER TABLE hadron_endpoints ADD COLUMN tenant_id VARCHAR, ADD COLUMN shard_count INTEGER;

UPDATE hadron_endpoints
SET tenant_id = hadron_metastores.tenant_id, shard_count = hadron_metastores.shard_count
FROM hadron_metastores
WHERE hadron_endpoints.metastore_id = hadron_metastores.metastore_id;

ALTER TABLE hadron_endpoints ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN shard_count SET NOT NULL;

-- We would still like to keep the `hadron_endpoints.metastore_id` column around. We will get rid
-- of the `hadron_metastores` table so remove the foreign key constraint to prevent `ON DELETE CASCADE`
-- from kicking in.
ALTER TABLE hadron_endpoints DROP CONSTRAINT hadron_endpoints_metastore_id_fkey;

-- Create a secondary index on `hadron_endpoints` on column `tenant_id` to speed up endpoint lookups by
-- tenant. This is needed by compute notifications.
CREATE INDEX hadron_idx_tenant_id_endpoint_id ON hadron_endpoints (tenant_id);

-- Dropping the table also drops any secondary indexes on the table.
DROP TABLE hadron_metastores;
