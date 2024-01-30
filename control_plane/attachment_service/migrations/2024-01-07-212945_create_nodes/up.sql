CREATE TABLE nodes (
  node_id BIGINT PRIMARY KEY NOT NULL,

  scheduling_policy VARCHAR NOT NULL,

  listen_http_addr VARCHAR NOT NULL,
  listen_http_port INTEGER NOT NULL,
  listen_pg_addr VARCHAR NOT NULL,
  listen_pg_port INTEGER NOT NULL
);