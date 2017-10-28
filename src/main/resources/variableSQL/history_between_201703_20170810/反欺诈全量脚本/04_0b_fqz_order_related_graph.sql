use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=5000000;
set mapreduce.job.queuename=szbigdata;

--区分实时、离线 --3667062811
insert overwrite  table fqz_order_related_graph_bu as
select
'1' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
'null' as c5,
'null' as c6,
'null' as c7,
'null' as order2,
'null' as performance2,
'null' as apply_time2,
'null' as type2,
0 as history_due_day2,
0 as current_due_day2,
'null' as cert_no2,
0 as label2
from fqz_order_data1 a   --33139394
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id  --	15588539
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
;

--订单关联s_c_loan_apply按loan_date分段跑数
create table  loan_apply_20170901 as
select * from creditloan.s_c_loan_apply f  where  f.year=2017 and f.month=09 and f.day=01 ;


drop table loan_apply_loandate_20160101before;
create table  loan_apply_loandate_20160101before as
select * from loan_apply_20170901   where   loan_date<'2016-01-01' ;

create table  loan_apply_loandate_20160101_20160501 as
select * from loan_apply_20170901   where  loan_date>='2016-01-01 00:00:00' and  loan_date<'2016-05-01' ;

create table  loan_apply_loandate_20160501_20161001 as
select * from loan_apply_20170901   where  loan_date>='2016-05-01 00:00:00' and  loan_date<'2016-10-01' ;

create table  loan_apply_loandate_20161001_20170301 as
select * from loan_apply_20170901   where  loan_date>='2016-10-01 00:00:00' and  loan_date<'2017-03-01' ;


create table  loan_apply_loandate_20170301_20170901 as
select * from loan_apply_20170901   where  loan_date>='2017-03-01 00:00:00' and  loan_date<'2017-09-01' ;

--三段分别插入目标表
insert into lkl_card_score.fqz_order_related_graph_bu
select
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.history_due_day as history_due_day2,
d.current_due_day as current_due_day2,
d.cert_no as cert_no2,
d.label as label2
from fqz_order_data2 a
join loan_apply_loandate_20160101before f  on f.order_id = a.c0  --1863791
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
join fqz_order_performance_data_new_0829 d on a.c8 = d.order_id;

insert into lkl_card_score.fqz_order_related_graph_bu
select
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.history_due_day as history_due_day2,
d.current_due_day as current_due_day2,
d.cert_no as cert_no2,
d.label as label2
from fqz_order_data2 a
join loan_apply_loandate_20160501_20161001 f  on f.order_id = a.c0 --5575345
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
join fqz_order_performance_data_new_0829 d on a.c8 = d.order_id;

insert into lkl_card_score.fqz_order_related_graph_bu
select
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.history_due_day as history_due_day2,
d.current_due_day as current_due_day2,
d.cert_no as cert_no2,
d.label as label2
from fqz_order_data2 a
join loan_apply_loandate_20161001_20170301 f  on f.order_id = a.c0 --8146632
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
join fqz_order_performance_data_new_0829 d on a.c8 = d.order_id;


insert into lkl_card_score.fqz_order_related_graph_bu
select
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.history_due_day as history_due_day2,
d.current_due_day as current_due_day2,
d.cert_no as cert_no2,
d.label as label2
from fqz_order_data2 a
join loan_apply_loandate_20170301_20170901 f  on f.order_id = a.c0  --1863791
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
join fqz_order_performance_data_new_0829 d on a.c8 = d.order_id;

insert into lkl_card_score.fqz_order_related_graph_bu
select
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.history_due_day as history_due_day_src,
b.current_due_day as current_due_day_src,
b.cert_no as cert_no_src,
b.label as label_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.history_due_day as history_due_day1,
c.current_due_day as current_due_day1,
c.cert_no as cert_no1,
c.label as label1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.history_due_day as history_due_day2,
d.current_due_day as current_due_day2,
d.cert_no as cert_no2,
d.label as label2
from fqz_order_data2 a
join loan_apply_loandate_20160101_20160501 f  on f.order_id = a.c0 --5575345
join fqz_order_performance_data_new_0829 b on a.c0 = b.order_id
join fqz_order_performance_data_new_0829 c on a.c4 = c.order_id
join fqz_order_performance_data_new_0829 d on a.c8 = d.order_id;
