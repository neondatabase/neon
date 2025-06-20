CREATE SCHEMA IF NOT EXISTS tenant2;

CREATE TABLE IF NOT EXISTS tenant2.items (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

INSERT INTO tenant2.items (name) VALUES 
    ('tenant2 item 1'),
    ('tenant2 item 2'),
    ('tenant2 item 3');


CREATE ROLE tenant2_role NOINHERIT;
GRANT ROLE tenant2_role TO authenticator;

GRANT USAGE ON SCHEMA tenant2 TO tenant2_role;
GRANT ALL ON ALL TABLES IN SCHEMA tenant2 TO tenant2_role;