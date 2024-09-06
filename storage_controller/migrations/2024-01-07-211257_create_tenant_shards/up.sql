CREATE TABLE tenant_shards (
  tenant_id VARCHAR NOT NULL,
  shard_number INTEGER NOT NULL,
  shard_count INTEGER NOT NULL,
  PRIMARY KEY(tenant_id, shard_number, shard_count),
  shard_stripe_size INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  generation_pageserver BIGINT NOT NULL,
  placement_policy VARCHAR NOT NULL,
  splitting SMALLINT NOT NULL,
  -- config is JSON encoded, opaque to the database.
  config TEXT NOT NULL
);