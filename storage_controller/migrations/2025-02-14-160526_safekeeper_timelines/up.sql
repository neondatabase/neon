CREATE TABLE timelines (
  tenant_id VARCHAR NOT NULL,
  timeline_id VARCHAR NOT NULL,
  start_lsn pg_lsn NOT NULL,
  generation INTEGER NOT NULL,
  sk_set BIGINT[] NOT NULL,
  new_sk_set BIGINT[],
  cplane_notified_generation INTEGER NOT NULL,
  deleted_at timestamptz,
  PRIMARY KEY(tenant_id, timeline_id)
);
CREATE TABLE safekeeper_timeline_pending_ops (
  sk_id BIGINT NOT NULL,
  tenant_id VARCHAR NOT NULL,
  timeline_id VARCHAR NOT NULL,
  generation INTEGER NOT NULL,
  op_kind VARCHAR NOT NULL,
  PRIMARY KEY(tenant_id, timeline_id, sk_id)
);
