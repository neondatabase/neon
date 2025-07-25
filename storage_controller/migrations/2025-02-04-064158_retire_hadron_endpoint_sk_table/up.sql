-- Merge the hadron_endpoint_safekeeper table into the hadron_timeline_safekeepers table by converting
-- every row in hadron_endpoint_safekeeper to a row in the hadron_timeline_safekeepers table.
-- We are also adding a nullable legacy_endpoint_id column to the hadron_timeline_safekeepers table to
-- make the migration (and its reversal if needed) easier.
ALTER TABLE hadron_timeline_safekeepers ADD COLUMN legacy_endpoint_id UUID DEFAULT NULL;

INSERT INTO hadron_timeline_safekeepers (timeline_id, sk_node_id, legacy_endpoint_id)
SELECT ep.timeline_id, rel.sk_node_id, rel.endpoint_id
FROM hadron_endpoint_safekeeper as rel
JOIN hadron_endpoints as ep ON rel.endpoint_id = ep.endpoint_id;

DROP TABLE hadron_endpoint_safekeeper;
