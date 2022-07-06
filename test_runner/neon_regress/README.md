To add a new SQL test

- add sql script to run to neon_regress/sql/testname.sql
- add expected output to neon_regress/expected/testname.out
- add testname to parallel_schedule

That's it.
For more complex tests see PostgreSQL regression tests. These works basically the same.
