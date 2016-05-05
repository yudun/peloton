create table t2 (id integer, name integer);
insert into t2 values (1,null);
alter table t2 alter column name SET NOT NULL;
insert into t2 values (2,null);
select * from t2;

create table t1 (id integer, name integer);
insert into t1 values (1,1);
alter table t1 alter column name SET NOT NULL;
insert into t1 values (2,null);
insert into t1 values (2,2);
select * from t1;
alter table t1 alter column name DROP NOT NULL;
insert into t1 values (3,null);
insert into t1 values (4,4);
select * from t1;

create table t3 (id integer, name integer);
insert into t3 values (1,2);
alter table t3 add constraint my unique(name);
insert into t3 values (2,2);
insert into t3 values (3,3);
select * from t3;
alter table t3 drop constraint my;
insert into t3 values (4,3);
select * from t3;
