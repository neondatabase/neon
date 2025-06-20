-- docker exec -it proxy-postgres psql -U postgres -c "CREATE SCHEMA IF NOT EXISTS neon_control_plane"
-- docker exec -it proxy-postgres psql -U postgres -c "CREATE TABLE neon_control_plane.endpoints (endpoint_id VARCHAR(255) PRIMARY KEY, allowed_ips VARCHAR(255))"
-- docker exec -it proxy-postgres psql -U postgres -c "CREATE ROLE proxy WITH SUPERUSER LOGIN PASSWORD 'password';"

CREATE SCHEMA IF NOT EXISTS neon_control_plane;

CREATE TABLE IF NOT EXISTS neon_control_plane.endpoints (
  endpoint_id VARCHAR(255) PRIMARY KEY,
  allowed_ips VARCHAR(255)
);

-- CREATE ROLE proxy WITH SUPERUSER LOGIN PASSWORD 'password';