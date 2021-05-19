To add a new SQL test

- add sql script to run to zenith_regress/sql/testname.sql
- add expected output to zenith/regress/expected/testname.out
- add testname to both parallel_schedule and serial_schedule files*

That's it.
For more complex tests see PostgreSQL regression tests. These works basically the same.

*it was changed recently in PostgreSQL upstream - no more separate serial_schedule.
Someday we'll catch up with these changes.
