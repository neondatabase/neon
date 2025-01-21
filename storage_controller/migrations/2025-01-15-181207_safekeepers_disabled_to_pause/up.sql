ALTER TABLE safekeepers ALTER COLUMN scheduling_policy SET DEFAULT 'pause';
UPDATE safekeepers SET scheduling_policy = 'pause' WHERE scheduling_policy = 'disabled';
