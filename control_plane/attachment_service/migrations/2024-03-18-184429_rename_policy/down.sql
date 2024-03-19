
UPDATE tenant_shards set placement_policy='{"Double": 1}' where placement_policy='{"Attached": 1}';
UPDATE tenant_shards set placement_policy='"Single"' where placement_policy='{"Attached": 0}';