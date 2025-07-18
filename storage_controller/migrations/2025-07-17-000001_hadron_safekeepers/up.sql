-- hadron_safekeepers keep track of all Safe Keeper nodes that exist in the system.
-- Upon startup, each Safe Keeper reaches out to the hadron cluster coordinator to register its node ID and listen addresses.

CREATE TABLE hadron_safekeepers (
  sk_node_id BIGINT PRIMARY KEY NOT NULL,
  listen_http_addr VARCHAR NOT NULL,
  listen_http_port INTEGER NOT NULL,
  listen_pg_addr VARCHAR NOT NULL,
  listen_pg_port INTEGER NOT NULL
);

CREATE TABLE hadron_timeline_safekeepers (
  timeline_id VARCHAR NOT NULL,
  sk_node_id BIGINT NOT NULL,
  legacy_endpoint_id UUID DEFAULT NULL,
  PRIMARY KEY(timeline_id, sk_node_id)
);
