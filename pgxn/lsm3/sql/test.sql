create extension lsm3;

create table t(k bigint, val bigint);
create index lsm3_index on t using lsm3(k);

set enable_seqscan=off;

insert into t values (1,10);
select lsm3_start_merge('lsm3_index');
select lsm3_wait_merge_completion('lsm3_index');
insert into t values (2,20);
select lsm3_start_merge('lsm3_index');
select lsm3_wait_merge_completion('lsm3_index');
insert into t values (3,30);
select lsm3_start_merge('lsm3_index');
select lsm3_wait_merge_completion('lsm3_index');
insert into t values (4,40);
select lsm3_start_merge('lsm3_index');
select lsm3_wait_merge_completion('lsm3_index');
insert into t values (5,50);
select lsm3_start_merge('lsm3_index');
select lsm3_wait_merge_completion('lsm3_index');
select lsm3_get_merge_count('lsm3_index');
select * from t where k = 1;
select * from t order by k;
select * from t order by k desc;
analyze t;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t order by k;

insert into t values (generate_series(1,100000), 1);
insert into t values (generate_series(1000001,200000), 2);
insert into t values (generate_series(2000001,300000), 3);
insert into t values (generate_series(1,100000), 1);
insert into t values (generate_series(1000001,200000), 2);
insert into t values (generate_series(2000001,300000), 3);
select * from t where k = 1;
select * from t where k = 1000000;
select * from t where k = 2000000;
select * from t where k = 3000000;
analyze t;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t where k = 1;
select lsm3_get_merge_count('lsm3_index') > 5;

truncate table t;
insert into t values (generate_series(1,1000000), 1);
select * from t where k = 1;

reindex table t;
select * from t where k = 1;

drop table t;

create table lsm(k bigint);
insert into lsm values (generate_series(1, 1000000));
create index concurrently on lsm using lsm3(k);
select * from lsm where k = 1;

drop table lsm;

