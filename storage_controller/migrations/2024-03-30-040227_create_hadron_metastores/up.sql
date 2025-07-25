-- hadron_metastores table stores the mapping from metastore to (tenant_id, shard_count),
-- which uniquely identifies a "tenant" in the Hadron storage cluster.

CREATE TABLE hadron_metastores (
    -- Databricks Metastore ID, known to the Databricks control plane.
    metastore_id UUID PRIMARY KEY NOT NULL,
    -- Hadron tenant ID. Only used in the data plane.
    tenant_id VARCHAR NOT NULL,
    shard_count INTEGER NOT NULL
);