CREATE SCHEMA IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.items (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

INSERT INTO test.items (name) VALUES 
    ('test item 1'),
    ('test item 2'),
    ('test item 3');

CREATE ROLE test_role NOLOGIN;
GRANT test_role TO authenticator;

GRANT USAGE ON SCHEMA test TO test_role;
GRANT ALL ON ALL TABLES IN SCHEMA test TO test_role;