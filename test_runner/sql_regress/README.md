Simple tests that only need a PostgreSQL connection to run.
These are run by the regress/test_pg_regress.py test, which uses
the PostgreSQL pg_regress utility.

To add a new SQL test:

- add sql script to run to neon_regress/sql/testname.sql
- add expected output to neon_regress/expected/testname.out
- add testname to parallel_schedule

That's it.
For more complex tests see PostgreSQL regression tests in src/test/regress.
These work basically the same.
