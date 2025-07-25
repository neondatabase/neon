-- Columns for storing the *owning* workspace ID and URL for Hadron endpoints.

ALTER TABLE hadron_endpoints ADD COLUMN workspace_id VARCHAR;
ALTER TABLE hadron_endpoints ADD COLUMN workspace_url VARCHAR;
