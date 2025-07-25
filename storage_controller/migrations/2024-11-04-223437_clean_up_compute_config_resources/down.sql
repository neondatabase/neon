-- The migration is non-reversible as a one-off task
RAISE EXCEPTION 'This migration is non-reversible. Please restore from backup.';
