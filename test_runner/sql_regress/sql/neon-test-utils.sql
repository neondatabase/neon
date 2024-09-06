-- Test the test utils in pgxn/neon_test_utils. We don't test that
-- these actually consume resources like they should - that would be
-- tricky - but at least we check that they don't crash.

CREATE EXTENSION neon_test_utils;

select test_consume_cpu(1);

select test_consume_memory(20); -- Allocate 20 MB
select test_release_memory(5);  -- Release 5 MB
select test_release_memory();   -- Release the remaining 15 MB
