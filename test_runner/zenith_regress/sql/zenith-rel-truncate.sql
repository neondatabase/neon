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


--
-- Test that the FSM is truncated along with the table.
--

-- Create a test table and delete and vacuum away most of the rows.
-- This leaves the FSM full of pages with plenty of space
create table tt(i int);
insert into tt select g from generate_series(1, 100000) g;
delete from tt where i%100 != 0 and i > 10000;
vacuum freeze tt;

-- Delete the rest of the rows, and vacuum again. This truncates the
-- heap to 0 blocks, and should also truncate the FSM.
delete from tt;
vacuum tt;

-- This can be used to look at the FSM directly, if the 'pg_freespace' contrib module
-- is installed
--SELECT blkno, avail from generate_series(1, 450) blkno, pg_freespace('tt'::regclass, blkno) AS avail;

-- Insert a row again. It should go on block #0. If the FSM was not truncated,
-- the insertion would find a higher-numbered block in the FSM and use that instead.
insert into tt values (0);
select ctid, * from tt;

drop table tt;
