--set autocommit = 0
set @@autocommit = 0;
set autocommit = 0;
set @@session.autocommit = 0;
set @@autocommit = 1;
set @@autocommit = 0, @@last_insert_id = 0;
set @@last_insert_id = 0, @@autocommit = 0;
set @@autocommit = 0, @@tx_read_only = 0;
set @@tx_read_only = 0, @@autocommit = 0;
set @@tx_read_only = 0, @@autocommit = 1;
--tx_read_only
select @@tx_read_only from DUAL;
select @@tx_read_only, @@tx_read_only;
select @@tx_read_only, @@tx_read_only from t1;
select @@tx_read_only, @@tx_read_only from DUAL;
select @@autocommit, @@tx_read_only;
select @@autocommit, @@tx_read_only from t1;
select @@autocommit, @@tx_read_only from DUAL;
select @@tx_read_only, @@tx_read_only, @@autocommit;
select @@tx_read_only, @@tx_read_only, @@autocommit from t1;
select @@tx_read_only, @@tx_read_only, @@autocommit from DUAL;
select @@tx_read_only, @@autocommit, @@tx_read_only;
select @@tx_read_only, @@autocommit, @@tx_read_only from t1;
select @@tx_read_only, @@autocommit, @@tx_read_only from DUAL;
select * from (select @@tx_read_only) tmp1;
select * from (select @@tx_read_only, id from t1) tmp1;
begin; set @@autocommit = 0; select * from t1;
begin; set @@autocommit = 0; set @@last_insert_id = 0; commit;
set @@autocommit = 0; begin; set @@autocommit = 0; select * from t1;
set @@autocommit = 0; begin;
begin; ;
begin; begin; set @@autocommit = 0; begin;
begin; select * from t1; commit;
set @@autocommit = 1; select * from t1; set @@autocommit = 0;
set @@autocommit = 1; set @@ob_query_timeout = 100000000; select * from t1; set @@autocommit = 0;
begin; insert into t1 values('1'); commit; begin; select * from t1; commit;
set @@autocommit = 0; begin;
set @@autocommit = 0; begin; select * from t1; set @@autocommit = 0;
begin; set @@autocommit = 0;
set @@init_connect="show tables;\
show databases;"
update /*+ hotspot */ t1 set c2='aaaa' where c1=1;
select "--- Test 1 Load from Dump binlog file --" as "";
SELECT UPPER((SELECT c2 FROM t1 WHERE c1=2)) FROM t2;
SELECT CONCAT(BIN(c1),BIN(c2),BIN(c3)) FROM t2;
SELECT fid, AsText(GeometryN(g, 1)) from gis_geometrycollection;
select * from (select * from table1) as t1 union ((select * from table2) as t2)
select row_id, row_hex, substr(row_hex,1,length(row_hex)), substring(row_hex,1,-1), concat(grp_id,"&",row_hex), trim(trailing row_hex from trim(leading grp_id from glike) ), upper(v1), lower(v1), unhex(hex(row_hex)), ip2int(int2ip(row_id)), repeat(row_hex,2), locate(row_hex,glike)=instr(glike,row_hex), substring_index(glike,concat('&',row_hex),1), substring_index(glike,concat(grp_id,'&'),-1), glike regexp row_hex as regexp1, glike rlike concat(grp_id,row_hex) as rlike1, gmt_create, gmt_modified from obright where grp_id=125604 and glike like concat('%',row_hex) and glike not like concat(row_hex,'%');
select count(*) from t1 where pk1=2 and pk2>3 and pk2<=6 and key1=2 and key2>2 and key2<6;
select row_id, CURRENT_TIME(6), CURRENT_TIMESTAMP(6), date_sub(date_add(t1, interval 1 second), interval 1 second), extract(SECOND from now())=extract(SECOND from CURRENT_TIMESTAMP), usec_to_time(time_to_usec(t1)), UNIX_TIMESTAMP(), datediff(now(),CURRENT_TIMESTAMP(6)), timediff(now(6),CURRENT_TIMESTAMP(6)), datediff(ts,gmt_modified), timediff(ts,gmt_modified), period_diff(DATE_FORMAT(now(), '%Y%m%d'),DATE_FORMAT(now(6), '%Y%m%d')), gmt_create, gmt_modified from OBRight where grp_id=117673;
start transaction;
start transaction ;
select pk1,pk2,i1,i2 from t1 where i1=decode(sign((i1+i2)*0.5-i1),1,i1,-1,i2,0,i1-i2);
/* jdbc */ start transaction ;
begin /* what the hell */ ;
start "123" transaction;
start count transaction;
,, begin;
start , transaction;
start transaction ,,,,,;
show start transaction;
select * from `阿里`;
select instr('阿里巴巴', '阿里');
select * from `什么`;
select count(*) from oceanbase.__all_server where zone = 'z2' and start_service_time <= 0;
select /*+ read_consistency(weak)*/ count(*) as oceanbase_cnt from __all_table where tenant_id=1 and database_id=(select database_id from __all_database where tenant_id=1 and database_name='oceanbase');
select /*+ READ_CONSISTENCY*/ * from `t1` where c1 = 'a' 'b' /*HINT+ READ_CONSISTENCY*/;
select "1", '123' from `t1`;
select "1", '123' from `from`;
select "1", '123' from errors;
select /*+ QUERY_TIMEOUT(100000)xxxx, xx */ from test;
#select /*+ QUERY_TIMEOUT(100000);
select /*+ QUERY_TIMEOUT(100000) */ * from t1;
SHOW STATUS LIKE 'Handler_read_%';
select * from (select * from table1 where clo1='x') where col2='y';
show errors;
show warnings limit 1;
show errors limit 1;
show count(*) warnings;
begin 123;
start transaction;
select * from t1.;
select found_rows();
select @@last_insert_id();
insert into t1(c1) values(1),(2);
insert into t1(c1) values(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2);
select * from t1;
select found_rows(), row_count(), last_insert_id() from t1 where c1 = 1;
select found_rows() from;
select * from t1 where c1 = 111111111111111111111111111;
select * from t1 where c1 = 'a' /*READ_CONSISTENCY*/;
select d.t1.c1, sum(t1.c2) from d.t1 where d.t1.c1 > 0 and c2 + d.t1.c1 = 100 group by d.t1.c2 order by t.d1.c1 desc limit 0, 1;
select * from t1 where c1 = 'abc';
create table s002 (c1 int primary key, c2 varchar(50), unique index idx(c2(20)));
create table s001 (c1 int primary key, c2 varchar(50), unique key idx(c2(20)));
create table s001 (c1 int primary key, c2 varchar(50), unique idx(c2(20)));
use rongxuan;
start transaction read only;
start transaction read write;
start transaction with consistent snapshot, read only;
start transaction read write, with consistent snapshot;
alter user 'zdy' account lock;
alter user 'zdy' account unlock ;
select d.t1.c1, sum(t1.c2) from d.t1 where d.t1.c1 > 0 and c2 + d.t1.c1 = 100 group by d.t1.c2 order by t.d1.c1 desc limit 0, 1;
select * from t1 where t1.c1 > 0 and c2 + t1.c1 = 100 group by t1.c2 order by t1.c1 desc limit 0, 1;
select c1, sum(d.t1.c2) from t1 where t1.c1 > 0 and c2 + t1.c1 = 100 group by t1.c2 order by t1.c1 desc limit 0, 1;
select t1.c1, sum(c2) from t1 where c1 > 0 and c2 + c1 = 100 group by c2 order by c1 desc limit 0, 1;
insert into t1 values(1, 2);
insert into t1.t1 values(1, 2);
insert into t1(c1) values(1), (2);
insert into t1(t1.c1) values(1), (2);
insert into d.t1(d.t1.c1) values(1), (2);
update t1 set d.t1.c2=t1.c1+1 where d.t1.c1 > 1 order by d.t1.c1 desc limit 0, 10;
update d.t1 set t1.c2=d.t1c1+1 where t1.c1 > 1 order by c1 desc limit 0, 10;
delete from d.t1 where d.t1.c2 > 10 order by c1 limit 0, 1;
select t1.c1, t2.c1 from d.t1 join d.t2 on d.t1.c1=t2.c1 where t1.c1>0;
select d.t1.c1, t2.c1 from d.t1 join t2 on t1.c1=t2.c1 where t1.c1>0;
select d.t1.c1, t2.c1 from d.t1 join t2 on c1=c1 where t1.c1>0;
insert into t1 value (1, 2), (3, 4) on duplicate key update d.t.c1 = t.c2 + 1, c2 = c2 + 3;
insert into d.t1 value (1, 2), (3, 4) on duplicate key update t.c1 = t.c2 + 1, d.t.c2 = t.c2 + 3;
create table rongxuan(c int primary key, c2 int);
create table rongxuan(rongxuan.c int primary key, c2 int);
create table rongxuan(d.rongxuan.c int primary key, c2 int);
drop table t1;
drop table oceanbase.t, t1;
alter table rongxuan add c3 int;
alter table rongxuan add rongxuan.c4 int;
alter table rongxuan add test.rongxuan.c5 int;
drop database rongxuan;
create database rongxuan;
create database if not exists rongxuan;
create database if not exists rongxuan default character set = 'utf8'  default collate = 'default_collate';
select * from d.t1 PARTITION(p1, p2);
delete from d.t1 PARTITION(p0, p1);
update d.t1 PARTITION (p2) SET id = 2 WHERE name = 'Jill';
INSERT INTO d.t1 PARTITION (p3, p4) VALUES (24, 'Tim', 'Greene', 3, 1),  (26, 'Linda', 'Mills', 2, 1);
REPLACE INTO d.t1 PARTITION (p0) VALUES (20, 'Jan', 'Jones', 3, 2);
SELECT e.id, s.city, d.name FROM e JOIN stores PARTITION (p1) ON e.id=s.id JOIN departments PARTITION (p0) ON e.id=d.id;
alter system switch replica leader zone = 'z1';
alter system switch replica leader server = '127.0.0.1:80';
alter system switch replica leader partition_id = '1%8@3001' server = '127.0.0.1:80';
alter system switch replica leader partition_id '1%8@3001' server '127.0.0.1:80';
alter system drop replica partition_id = '1%8@3001' server = '127.0.0.1:80';
alter system drop replica partition_id = '1%8@3001' server = '127.0.0.1:80' create_timestamp = 1;
alter system drop replica partition_id = '1%8@3001' server = '127.0.0.1:80' zone = 'z1';
alter system drop replica partition_id = '1%8@3001' server = '127.0.0.1:80' create_timestamp = 1 zone = 'z1';
alter system drop replica partition_id '1%8@3001' server '127.0.0.1:80' create_timestamp 1 zone 'z1';
alter system copy replica partition_id = '1%8@3001' source = '127.0.0.1:80' destination = '127.0.0.2:80';
alter system move replica partition_id = '1%8@3001' source = '127.0.0.1:80' destination = '127.0.0.2:80';
alter system move replica partition_id '1%8@3001' source '127.0.0.1:80' destination '127.0.0.2:80';
alter system report replica;
alter system report replica server = '127.0.0.1:80';
alter system report replica zone = 'z1';
alter system recycle replica;
alter system recycle replica server = '127.0.0.1:80';
alter system recycle replica server '127.0.0.1:80';
alter system recycle replica zone = 'z1';
alter system recycle replica zone 'z1';
alter system major freeze;
alter system start merge zone = 'z1';
alter system suspend merge;
alter system suspend merge zone = 'z1';
alter system resume merge;
alter system resume merge zone = 'z1';
alter system clear roottable;
alter system clear roottable tenant = 'xxx';
select * from t1 where c1>ANY(select c1 from t2 where c2>1);
select * from t1 where c1>SOME(select c1 from t2 where c2>1);
select * from t1 where c1>ALL(select c1 from t2 where c2>1);
select * from t1 where c1>(select c1 from t2 where c2>1);
select * from t1 where c1<ANY(select c1 from t2 where c2>1);
select * from t1 where c1<SOME(select c1 from t2 where c2>1);
select * from t1 where c1<ALL(select c1 from t2 where c2>1);
select * from t1 where c1<(select c1 from t2 where c2>1);
select * from t1 where c1>=ANY(select c1 from t2 where c2>1);
select * from t1 where c1>=SOME(select c1 from t2 where c2>1);
select * from t1 where c1>=ALL(select c1 from t2 where c2>1);
select * from t1 where c1>=(select c1 from t2 where c2>1);
select * from t1 where c1<=ANY(select c1 from t2 where c2>1);
select * from t1 where c1<=SOME(select c1 from t2 where c2>1);
select * from t1 where c1<=ALL(select c1 from t2 where c2>1);
select * from t1 where c1<=(select c1 from t2 where c2>1);
select * from t1 where c1=ANY(select c1 from t2 where c2>1);
select * from t1 where c1=SOME(select c1 from t2 where c2>1);
select * from t1 where c1=ALL(select c1 from t2 where c2>1);
select * from t1 where c1=(select c1 from t2 where c2>1);
select * from t1 where c1!=ANY(select c1 from t2 where c2>1);
select * from t1 where c1!=SOME(select c1 from t2 where c2>1);
select * from t1 where c1!=ALL(select c1 from t2 where c2>1);
select * from t1 where c1!=(select c1 from t2 where c2>1);
select * from t1 where c1 in (select c1 from t2 where c2>1);
select * from t1 where c1 not in (select c1 from t2 where c2>1);
select * from t1 where exists (select c1 from t2 where c2>1);
select * from t1 where not exists (select c1 from t2 where c2>1);
select * from t1 where (select c1 from t1) like (select c2 from t2);
select * from t1 where (select c1 from t1) not like (select c2 from t2);
select * from t1 where (c1) in (select c1 from t2);
select * from t1 where (c1, c2) in (select c1, c2 from t2);
select * from t1 where ROW(c1, c2) in (select c1, c2 from t2);
set names latin1;
set names 'latin1';
set names utf8 collate 'utf8_general_ci';
set names utf8 collate utf8_general_ci;
set character set utf8;
set character set 'utf8';
set charset utf8;
select _utf8 'abc', _utf8mb4 'def' collate utf8mb4_general_ci from t1 where c1 collate utf8_bin = 'xyz' collate utf8_bin;
select * from t1 where c1=?;
select * from t1 where c1>?;
select * from t1 where (select c1 from t1 where c1 = ?) not like (select c2 from t2 where c2=?);
#test join syntax with no  join_condition;
select * from t1 join t2;
select * from t1 inner join t2;
select * from t1 cross join t2;
select * from t1 cross join t2 join t3;
select "1234";
select '1234';
create table test(c1 varchar(3) binary);
create table test(c1 varchar(3) binary charset utf8mb4);
replace into test values(1,2);
insert ignore into test values(1,2);
insert ignore into test values(1,2) on duplicate key update c2 = c1 + 1;
