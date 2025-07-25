-- The hadron_endpoints table stores endpoints in the system. Each endpoint represents a PG "instance", which
-- includes both compute and storage. The endpoint_id is generated and known to the Databricks control plane.
-- The Databricks control plane has the source-of-truth of endpoint IDs.

-- An endpoint exists under a metastore/tenant (it actually exists under a workspace, but the data plane does
-- not have this concept), but endpoint IDs are globally unique.

CREATE TABLE hadron_endpoints (
  -- The Databricks control plane endpoint ID of the endpoint.
  endpoint_id UUID PRIMARY KEY NOT NULL,
  -- The Databricks metastore ID of the endpoint. We can find out the tenant ID by x-refing the hadron_metastores table.
  metastore_id UUID NOT NULL REFERENCES hadron_metastores (metastore_id) ON DELETE CASCADE,
  -- The Hadron timeline ID of the endpoint.
  timeline_id VARCHAR NOT NULL,
  -- The "compute index" of the primary (read/write) compute node of the endpoint. (endpoint_id, compute_index) identifies
  -- the compute node in the hadron_computes table.
  primary_compute_index INTEGER NOT NULL,
  -- NULL means the endpoint is not deleted. Deleted endpoints are kept in the database for recovery purposes.
  deleted_at TIMESTAMP WITH TIME ZONE
);