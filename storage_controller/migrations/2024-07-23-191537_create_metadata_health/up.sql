CREATE TABLE metadata_health (
  tenant_id VARCHAR NOT NULL,
  shard_number INTEGER NOT NULL,
  shard_count INTEGER NOT NULL,
  PRIMARY KEY(tenant_id, shard_number, shard_count),
  -- Rely on cascade behavior for delete
  FOREIGN KEY(tenant_id, shard_number, shard_count) REFERENCES tenant_shards ON DELETE CASCADE,
  healthy BOOLEAN NOT NULL DEFAULT TRUE,
  last_scrubbed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


INSERT INTO metadata_health(tenant_id, shard_number, shard_count)
SELECT tenant_id, shard_number, shard_count FROM tenant_shards;
