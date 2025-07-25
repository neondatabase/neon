CREATE TABLE hadron_timeline_safekeepers (
  timeline_id VARCHAR NOT NULL,
  sk_node_id BIGINT NOT NULL,
  PRIMARY KEY(timeline_id, sk_node_id)
);
