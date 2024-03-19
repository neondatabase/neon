
UPDATE tenant_shards set placement_policy='{"Attached": 1}' where placement_policy='{"Double": 1}';
UPDATE tenant_shards set placement_policy='{"Attached": 0}' where placement_policy='"Single"';