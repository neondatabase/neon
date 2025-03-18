CREATE TABLE timeline_imports (
  tenant_id VARCHAR NOT NULL,
  timeline_id VARCHAR NOT NULL,
  shard_statuses JSONB NOT NULL,
  PRIMARY KEY(tenant_id, timeline_id)
);
