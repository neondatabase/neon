ALTER TABLE safekeepers ALTER COLUMN scheduling_policy SET DEFAULT 'disabled';
UPDATE safekeepers SET scheduling_policy = 'disabled' WHERE scheduling_policy = 'pause';
