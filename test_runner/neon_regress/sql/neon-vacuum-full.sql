create table foo(a int primary key, b int, c int);
insert into foo values (generate_series(1,10000), generate_series(1,10000), generate_series(1,10000));
create index concurrently on foo(b);
create index concurrently on foo(c);
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
vacuum full foo;
\d foo
drop table foo;
