CREATE TABLE tenant_shards (
  id INTEGER PRIMARY KEY NOT NULL,
  tenant_id VARCHAR NOT NULL,
  shard_number INTEGER NOT NULL,
  shard_count INTEGER NOT NULL,
  shard_stripe_size INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  placement_policy VARCHAR NOT NULL,
  -- config is JSON encoded, opaque to the database.
  config TEXT NOT NULL
);