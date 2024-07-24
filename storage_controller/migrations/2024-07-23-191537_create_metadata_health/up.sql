CREATE TABLE metadata_health (
  tenant_id VARCHAR NOT NULL,
  shard_number INTEGER NOT NULL,
  shard_count INTEGER NOT NULL,
  PRIMARY KEY(tenant_id, shard_number, shard_count),
  healthy BOOLEAN NOT NULL,
  last_scrubbed_at TIMESTAMPTZ NOT NULL
);
