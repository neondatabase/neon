CREATE TABLE timelines (
  tenant_id VARCHAR NOT NULL,
  timeline_id VARCHAR NOT NULL,
  PRIMARY KEY(tenant_id, timeline_id),
  generation INTEGER NOT NULL,
  sk_set BIGINT[] NOT NULL,
  new_sk_set BIGINT[] NOT NULL,
  cplane_notified_generation INTEGER NOT NULL,
  status_kind VARCHAR NOT NULL,
  status VARCHAR NOT NULL
);
CREATE INDEX timelines_idx ON timelines(status_kind, tenant_id, timeline_id);
