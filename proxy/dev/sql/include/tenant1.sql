CREATE SCHEMA IF NOT EXISTS tenant1;

CREATE TABLE IF NOT EXISTS tenant1.items (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

INSERT INTO tenant1.items (name) VALUES 
    ('tenant1 item 1'),
    ('tenant1 item 2'),
    ('tenant1 item 3');


CREATE ROLE tenant1_role NOINHERIT;
GRANT ROLE tenant1_role TO authenticator;

GRANT USAGE ON SCHEMA tenant1 TO tenant1_role;
GRANT ALL ON ALL TABLES IN SCHEMA tenant1 TO tenant1_role;