-- The hadron_computes table stores information about PG compute nodes that exist in the system.
-- Existence of a compute node entry in this table does not guarantee that the compute node is
-- running. Whether the node is running is governed by state in Kubernetes. This table indicates
-- whether a compute node's config exists and is ready to be spun up.

CREATE TABLE hadron_computes (
  -- The endpoint ID associated with the PG compute node.
  endpoint_id UUID NOT NULL,
  -- The index of the compute node in the endpoint. If index == 0, it is the primary compute node.
  -- If index > 0, it is a read-only compute node.
  compute_index INTEGER NOT NULL,
  -- The Kubernetes object name used for this compute.
  compute_name VARCHAR NOT NULL UNIQUE,
  -- JSON encoded compute config. Includes PG and resource (cpu/mem) parameters. Opaque to the database.
  compute_config TEXT NOT NULL,
  PRIMARY KEY (endpoint_id, compute_index)
);