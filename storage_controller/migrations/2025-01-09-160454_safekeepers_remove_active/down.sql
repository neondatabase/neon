-- this sadly isn't a "true" revert of the migration, as the column is now at the end of the table.
-- But preserving order is not a trivial operation.
-- https://wiki.postgresql.org/wiki/Alter_column_position
ALTER TABLE safekeepers ADD active BOOLEAN NOT NULL DEFAULT false;
