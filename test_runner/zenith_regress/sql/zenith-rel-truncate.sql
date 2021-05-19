--
-- Test that when a relation is truncated by VACUUM, the next smgrnblocks()
-- query to get the relation's size returns the new size.
-- (This isn't related to the TRUNCATE command, which works differently,
-- by creating a new relation file)
--
CREATE TABLE truncatetest (i int);
INSERT INTO truncatetest SELECT g FROM generate_series(1, 10000) g;

-- Remove all the rows, and run VACUUM to remove the dead tuples and
-- truncate the physical relation to 0 blocks.
DELETE FROM truncatetest;
VACUUM truncatetest;

-- Check that a SeqScan sees correct relation size (which is now 0)
SELECT * FROM truncatetest;

DROP TABLE truncatetest;
