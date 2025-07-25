-- Reverse the changes made in the up.sql file by:
-- 1. Recreating the hadron_timeline_safekeepers table in its original form.
-- 2. Find all rows from the hadron_timeline_safekeepers that have a non-null legacy_endpoint_id column,
--    and move these rows into the hadron_endpoint_safekeeper table.
-- 3. Drop the legacy_endpoint_id column from the hadron_timeline_safekeepers table.
CREATE TABLE hadron_endpoint_safekeeper (
  endpoint_id UUID NOT NULL,
  sk_node_id BIGINT NOT NULL,
  PRIMARY KEY (endpoint_id, sk_node_id),
  FOREIGN KEY (endpoint_id) REFERENCES hadron_endpoints (endpoint_id) ON DELETE CASCADE,
  FOREIGN KEY (sk_node_id) REFERENCES hadron_safekeepers (sk_node_id) ON DELETE CASCADE
);

INSERT INTO hadron_endpoint_safekeeper (endpoint_id, sk_node_id)
SELECT legacy_endpoint_id, sk_node_id
FROM hadron_timeline_safekeepers
WHERE legacy_endpoint_id IS NOT NULL;

DELETE FROM hadron_timeline_safekeepers WHERE legacy_endpoint_id IS NOT NULL;
ALTER TABLE hadron_timeline_safekeepers DROP COLUMN legacy_endpoint_id;
