-- The migration is non-reversible because in the old schema there is a one-to-one mapping from
-- metastores to tenants. This no longer holds in the new model where metastores and tenants are
-- completely decoupled.
RAISE EXCEPTION 'This migration is non-reversible. Please restore from backup.';
