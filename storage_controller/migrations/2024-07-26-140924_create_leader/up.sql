CREATE TABLE controllers (
  address VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(address, started_at)
);
