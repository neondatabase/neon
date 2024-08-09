CREATE TABLE controllers (
  hostname VARCHAR NOT NULL,
  port INTEGER NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(hostname, port, started_at)
);
