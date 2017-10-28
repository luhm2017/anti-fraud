use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;

--区分实时、离线
insert overwrite table fqz_order_related_graph 
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
from fqz_order_data1 a   -- 
join fqz_order_performance_data_new b on a.c0 = b.order_id
join fqz_order_performance_data_new c on a.c4 = c.order_id
union all
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
join fqz_order_performance_data_new b on a.c0 = b.order_id
join fqz_order_performance_data_new c on a.c4 = c.order_id
join fqz_order_performance_data_new d on a.c8 = d.order_id;
