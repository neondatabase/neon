-- The secondary index hadron_idx_tenant_id_metastore_id is used to quickly find the
-- metastore_id given a tenant_id in the hadron_metastores table.
CREATE INDEX hadron_idx_tenant_id_metastore_id ON hadron_metastores (tenant_id);
