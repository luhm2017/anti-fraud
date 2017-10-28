--1_fqz_contract_performance_data.sql
--所有合同表现，以creditloan.r_overdue_period为依据    历史逾期天数、当前逾期天数
--================================================================================
drop table fqz_contract_performance_data;
create table fqz_contract_performance_data as 
select ta.order_id,
ta.apply_time,
nvl(ta.performance,0) as history_due_day,
nvl(tb.performance,0) as current_due_day 
from (
--所有订单历史逾期天数
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --最大天数
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 06 and a.day = 26
and a.stat_type = 0  -- 0 表示时间
and a.due_time < '2017-06-26' --未超过还款截止时间
group by a.order_id,a.apply_time
) ta left join (
--所有订单当前逾期天数
select 
a.order_id,
a.apply_time,
max(cast(a.stat_count as int)) as performance --最大天数
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 06 and a.day = 26
and a.stat_type = 0 -- 0 表示时间
and a.due_time < '2017-06-26' --当前日期超过还款截止时间
and a.overdue_type = 0  --未还清
group by a.order_id,a.apply_time) tb
on ta.order_id = tb.order_id;

--2_fqz_order_performance_data
--所有订单表现数据 ，（所有合同以及拒绝的进件）
--================================================================================
drop table fqz_order_performance_data;
create table fqz_order_performance_data as 
select 
ORDER_ID,
case when FAIL_REASON like '%Q%' THEN 'q_refuse' ELSE 'other_refuse' end as performance,
LOAN_DATE as apply_time,
'refuse' as type,
0 as  history_due_day,
0 as current_due_day
from creditloan.s_c_loan_apply a
where a.status = 'F'
and a.year = 2017 
and a.month = 06 
and day = 26
union all
select
r.order_id,
'null' as performance,
r.apply_time,
'pass' as type,
r.history_due_day,
r.current_due_day
from fqz_contract_performance_data r;

--3_fqz_order_performance_data_new.sql
--所有订单表现数据 ，（所有合同以及拒绝的进件） 新增身份证号、是否黑合同
--================================================================================
drop table fqz_order_performance_data_new;
create table fqz_order_performance_data_new as 
select f.*,u.cert_no,
(case when fb.orderno is not null then 1   -- 黑合同
when  (fb.orderno is null and f.type = 'pass' 
and nvl(f.history_due_day,0) <= 0 and nvl(f.current_due_day,0) <= 0 )then 0  -- 没有逾期的合同 0
else 2 end) as label --其它的为 2
from fqz_order_performance_data f 
join creditloan.s_c_loan_apply  a on f.order_id = a.order_id
left join fqz_black_contract fb on f.order_id = fb.orderno
join creditloan.s_c_apply_user u on u.id = a.id
where a.year = 2017 and a.month = 06 and a.day = 26
and u.year = 2017 and u.month = 06 and u.day = 26;


--4_1_fqz_order_related_graph.sql
--关系图谱数据 
--================================================================================
--全量坏样本:
--fqz_black_order_data1
--fqz_black_order_data2
--201608--201702的正样本:
--fqz_order1608to1702_case_data1
--fqz_order1608to1702_case_data2

--1、先排除fqz_order1608to1702_case_data1、fqz_order1608to1702_case_data2黑合同
--2、全量union all
--一度关联
create table fqz_order_data1 as 
select a.* from fqz_order1608to1702_case_data1 a
left join fqz_black_order_data1 b on a.c0 = b.c0
where b.c0 is null  --取全量白合同
union all 
select * from fqz_black_order_data1 ;

--二度关联
set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
--跑不动
create table fqz_order_data2 as 
select a.* from fqz_order1608to1702_case_data2 a
left join fqz_black_order_data2 b on a.c0 = b.c0
where b.c0 is null  --取全量白合同
union all 
select * from fqz_black_order_data2 ;

--另外一种方式 new
create table fqz_order_data2 as 
select tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8 from (
select * from fqz_order1608to1702_case_data2
union all
select * from fqz_black_order_data2) tab
group by tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8;

--4_2_fqz_order_related_graph.sql   --2563026279
--关系图谱数据，关联所有表现字段
--验证数据：select count(cnt ) from 
--(select distinct order_src as cnt  from fqz_order_related_graph where label_src = 1) tab
--26958
--============================================
drop table fqz_order_related_graph;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;

create table fqz_order_related_graph as
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

--5_overdue_data.sql
--基于关系图谱数据的清洗
--================================================================================
--一度关联自身
drop table overdue_cnt_self;
create table overdue_cnt_self as 
-- 一度关联自身_订单数量     
SELECT a.order_src,'order_cnt' title, 
count(distinct a.order1) cnt 
FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' 
and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by  a.order_src    
union all 
-- 一度关联自身_ID数量  
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src 
union all
-- 一度关联自身_黑合同数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1 and a.label1 = 1
group by a.order_src
-- 一度关联自身_Q标拒绝数量  1  
union all 
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现  
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src 
union all
-- 一度关联自身_通过合同数量 
SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src ;

--合并数据  一度关联自身
drop table  lkl_card_score.overdue_cnt_self_sum;
create table  lkl_card_score.overdue_cnt_self_sum  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt_self ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt_self ,   
sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt_self , 
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt_self,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt_self 
from  overdue_cnt_self
group by order_src;


--一度关联自身
-- 一度_当前无逾期数量        
-- 一度_当前3+数量            
-- 一度_当前30+数量 
-- 一度_历史无逾期数量        
-- 一度_历史3+数量            
-- 一度_历史30+数量            
drop table overdue_cnt_2_self_tmp;
create table overdue_cnt_2_self_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1<=0  --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>3  --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联自身
   and c.cert_no_src = c.cert_no1
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>30 --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1
  group by c.order_src 
union all
--历史逾期
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1<=0  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>3 --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联自身
   and c.cert_no_src = c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>30  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联自身
   and c.cert_no_src = c.cert_no1
   group by c.order_src;
   
   
--一度关联排除自身   
--================================================================================================

drop table overdue_cnt_1;
create table overdue_cnt_1 as 
-- 一度_订单数量  4  
SELECT a.order_src,'order_cnt' title, 
count(distinct a.order1) cnt 
FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' 
and a.apply_time_src>a.apply_time1
--一度关联排除自身
and a.cert_no_src <> a.cert_no1
group by  a.order_src    
union all 
-- 一度_ID数量    
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
--一度关联排除自身
and a.cert_no_src <> a.cert_no1
group by a.order_src 
union all
-- 一度关联自身_黑合同数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src <> a.cert_no1 and a.label1 = 1
group by a.order_src
-- 一度_Q标拒绝数量  1  
union all 
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现  
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1
--一度关联排除自身
and a.cert_no_src <> a.cert_no1
group by a.order_src 
union all
-- 一度_通过合同数量  2  
SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1
--一度关联排除自身
and a.cert_no_src <> a.cert_no1
group by a.order_src ;

drop table  lkl_card_score.overdue_cnt_1_sum;
create table  lkl_card_score.overdue_cnt_1_sum  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,
sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt ,    
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1
group by order_src;

--一度关联排除自身
-- 一度_当前无逾期数量        
-- 一度_当前3+数量            
-- 一度_当前30+数量 
-- 一度_历史无逾期数量        
-- 一度_历史3+数量            
-- 一度_历史30+数量    
drop table overdue_cnt_2_tmp;
create table overdue_cnt_2_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1<=0  --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>3 --当前 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day1>30 --当前
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1
  group by c.order_src 
union all
--历史逾期
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1<=0  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>3 --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1   
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order1) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day1>30  --历史
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   --一度关联排除自身
   and c.cert_no_src <> c.cert_no1
   group by c.order_src;
   

--合并一度关联 逾期数据(排除自身)
drop table  lkl_card_score.overdue_cnt_2;
create table  lkl_card_score.overdue_cnt_2  as
select order_src,
sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_tmp
group by order_src;

--合并一度关联 逾期数据(关联自身)
drop table  lkl_card_score.overdue_cnt_2_self;
create table  lkl_card_score.overdue_cnt_2_self  as
select order_src,
sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_self_tmp
group by order_src;
 
--==================================================================================================== 
-- 2度关联数据
drop table overdue_cnt_1_2;
create table overdue_cnt_1_2 as 
-- 2度_订单数量     
SELECT a.order_src,'order_cnt' title, 
count(distinct a.order2) cnt 
FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前
and a.apply_time_src>a.apply_time1
group by  a.order_src    
union all 
-- 2度_ID数量     
SELECT a.order_src,'id_cnt' title,
count(distinct a.cert_no2) cnt 
FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前
and a.apply_time_src>a.apply_time1
group by a.order_src 
union all
-- 2度_黑合同数量
SELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 
and a.apply_time_src>a.apply_time1
and a.label2 = 1
group by a.order_src
-- 2度_Q标拒绝数量  2  
union all 
SELECT a.order_src,'q_refuse_cnt' title,
count(distinct a.order2) cnt 
FROM fqz_order_related_graph a     --order订单表现  
where performance2='q_refuse' 
and a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 
and a.apply_time_src>a.apply_time1
group by a.order_src 
union all
-- 2度_通过合同数量  2  
SELECT a.order_src,'pass_cnt' title,
count(distinct a.order2) cnt 
FROM fqz_order_related_graph a     --order订单表现
where a.type2='pass'  
and a.degree_type='2' 
and a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前
and a.apply_time_src>a.apply_time1
group by a.order_src ;


--合并二度关联数据 
drop table overdue_cnt_2_sum;
create table  lkl_card_score.overdue_cnt_2_sum  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   
sum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt , 
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1_2
group by order_src;

--二度关联排除自身
-- 二度_当前无逾期数量        
-- 二度_当前3+数量            
-- 二度_当前30+数量 
-- 二度_历史无逾期数量        
-- 二度_历史3+数量            
-- 二度_历史30+数量
 
drop table overdue_cnt_2_2_tmp;
create table overdue_cnt_2_2_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.current_due_day2<=0  --当前
   and c.degree_type='2' 
   and c.apply_time_src>c.apply_time1  
   and c.apply_time_src>c.apply_time2 
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.current_due_day2>3 --当前 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.current_due_day2>30  --当前
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2 
  group by c.order_src 
union all
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.history_due_day2<=0 --历史
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.history_due_day2> 3 --历史
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
   and c.apply_time_src>c.apply_time2 
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order2) cnt 
       from 
   fqz_order_related_graph c 
 where c.type2='pass'       --通过 
   and c.history_due_day2> 30 --历史
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
   and c.apply_time_src>c.apply_time2    
   group by c.order_src
   ;
   
--合并二度关联 逾期数据
drop table  lkl_card_score.overdue_cnt_2_2;
create table  lkl_card_score.overdue_cnt_2_2  as
select order_src,
sum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           
sum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   
sum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  
sum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, 
sum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,
sum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls 
from  overdue_cnt_2_2_tmp
group by order_src;

--取全量样本order_src
drop table order_src_group;
create table order_src_group as  
select trim(order_src) order_src   
from 
( select c0 as order_src from fqz_order_data1
union all
select c0 as order_src from   fqz_order_data2
) a   
group by trim(order_src); 


--=====================================================================
--宽表合并
drop table overdue_result_all;
create table overdue_result_all as 
select 
a.order_src,
nvl(b.order_cnt_self,0) as order_cnt_self,
nvl(b.id_cnt_self,0) as id_cnt_self,
nvl(b.black_cnt_self,0) as black_cnt_self,
nvl(b.q_refuse_cnt_self,0) as q_refuse_cnt_self,
nvl(b.pass_cnt_self,0) as pass_cnt_self,
nvl(c.overdue0,0) as overdue0_self,
nvl(c.overdue3,0) as overdue3_self,
nvl(c.overdue30,0) as overdue30_self,
nvl(c.overdue0_ls,0) as overdue0_ls_self,
nvl(c.overdue3_ls,0) as overdue3_ls_self,
nvl(c.overdue30_ls,0) as overdue30_ls_self,
nvl(d.order_cnt,0) as order_cnt,
nvl(d.id_cnt,0) as id_cnt,
nvl(d.black_cnt,0) as black_cnt,
nvl(d.q_refuse_cnt,0) as q_refuse_cnt,
nvl(d.pass_cnt,0) as pass_cnt,
nvl(e.overdue0,0) as overdue0,
nvl(e.overdue3,0) as overdue3,
nvl(e.overdue30,0) as overdue30,
nvl(e.overdue0_ls,0) as overdue0_ls ,
nvl(e.overdue3_ls,0) as overdue3_ls,
nvl(e.overdue30_ls,0) as overdue30_ls, 
nvl(f.order_cnt,0) as order_cnt_2,
nvl(f.id_cnt,0)       as id_cnt_2,
nvl(f.black_cnt,0)       as black_cnt_2,
nvl(f.q_refuse_cnt,0) as q_refuse_cnt_2,
nvl(f.pass_cnt,0)    as pass_cnt_2,
nvl(g.overdue0,0)    as overdue0_2,
nvl(g.overdue3,0)    as overdue3_2,
nvl(g.overdue30,0)   as overdue30_2,
nvl(g.overdue0_ls,0) as overdue0_ls_2,
nvl(g.overdue3_ls,0) as overdue3_ls_2,
nvl(g.overdue30_ls,0) as overdue30_ls_2
from order_src_group a   --256441
left join overdue_cnt_self_sum b on a.order_src = b.order_src  -- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_self c on a.order_src = c.order_src    -- 合并一度关联 逾期数据(关联自身)
left join overdue_cnt_1_sum d on a.order_src = d.order_src     -- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2  e on a.order_src = e.order_src        -- 一度关联 逾期数据(排除自身)
left join overdue_cnt_2_sum f on a.order_src = f.order_src     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）
left join overdue_cnt_2_2 g on a.order_src = g.order_src;      -- 二度关联 逾期数据

--数据处理，过滤全0值
--=====================================================================
insert overwrite table overdue_result_all 
select * from overdue_result_all
where !(
	order_cnt_self	       = 0 and 
	id_cnt_self		       = 0 and 
  black_cnt_self		   = 0 and
	q_refuse_cnt_self	   = 0 and 
	pass_cnt_self	       = 0 and 
	overdue0_self	       = 0 and 
	overdue3_self	       = 0 and 
	overdue30_self	       = 0 and 
	overdue0_ls_self 	   = 0 and 
	overdue3_ls_self 	   = 0 and 
	overdue30_ls_self	   = 0 and 
	order_cnt		       = 0 and 
	id_cnt		           = 0 and 
  black_cnt     		   = 0 and
	q_refuse_cnt	       = 0 and 
	pass_cnt		       = 0 and 
	overdue0		       = 0 and 
	overdue3		       = 0 and 
	overdue30		       = 0 and 
	overdue0_ls		       = 0 and 
	overdue3_ls		       = 0 and 
	overdue30_ls	       = 0 and 
	order_cnt_2		       = 0 and 
	id_cnt_2		       = 0 and
  black_cnt_2     		   = 0 and 
	q_refuse_cnt_2	       = 0 and 
	pass_cnt_2		       = 0 and 
	overdue0_2		       = 0 and 
	overdue3_2		       = 0 and 
	overdue30_2		       = 0 and 
	overdue0_ls_2	       = 0 and 
	overdue3_ls_2	       = 0 and 
	overdue30_ls_2	       = 0
);


--取图关联边数据
--==================================================================
drop table order_src_bian_tmp;
create table order_src_bian_tmp as 
select 
a.order_src,
concat(a.c1,'|',a.c3) as ljmx,
1 as depth 
from fqz_order_related_graph a
where a.degree_type = '1' 
and a.apply_time_src>a.apply_time1 
--一度关联进件为黑
and a.label1 = 1 
union all 
select 
a.order_src,
concat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,
2 as depth 
from fqz_order_related_graph a
where a.degree_type = '2' 
and a.apply_time_src>a.apply_time1 
and a.apply_time_src>a.apply_time2    
--二度关联进件为黑
and a.label2 = 1;

--===========聚合关联边
drop table order_src_bian;   
create table order_src_bian as 
select c.order_src,concat_ws(',',collect_set(ljmx)) as ljmx           
from  order_src_bian_tmp  c
group by c.order_src;

--边字段 关联到结果表 
drop table overdue_result_all_new;
create table overdue_result_all_new as 
select 
c.label,
c.apply_time,
a.*,b.ljmx
from overdue_result_all a
left join order_src_bian b on a.order_src=b.order_src
join fqz_order_performance_data_new c on a.order_src = c.order_id;

--=========================================================================
--样本分布
select label,count(*) form overdue_result_all_new
group by label;
--0	140806
--1	17878

--WOE处理
--统计有边数据
select count(*)  from overdue_result_all_new 
where ljmx is not null ;  -- 5596

--=========================================================================
--提取边数据
drop table fqz_edge_data;
create table fqz_edge_data as 
select order_src,label,(split(ljmx,',')) as egdes 
from overdue_result_all_new where ljmx is not null
and label <> 2;

--统计关联边、WOE处理
drop table fqz_edge_data_total;
create table fqz_edge_data_total as 
select order_src,label,edge 
from fqz_edge_data fe lateral view explode(fe.egdes) adtable as edge;

--woe处理
create table fqz_edge_woe as 
select
edge,
nvl(ln((good_cnt/12527)/(bad_cnt/29046)),0) as woe
from (
select edge,
count(*) as cnt,
sum(case when label = 0 then 1 else 0 end) as bad_cnt,
sum(case when label = 1 then 1 else 0 end) as good_cnt
from fqz_edge_data_total
group by edge) tab;

--添加关联边的深度depth
--=====================
create table fqz_edge_depth as
select order_src,max(depth) as depth from order_src_bian_tmp 
group by order_src;

--统计每个订单边权重
create table fqz_order_edge_woe as 
select 
a.order_src,
sum(b.woe) as edge_woe_sum,
max(woe) as edge_woe_max,
min(woe) as edge_woe_min
from 
fqz_edge_data_total a 
join fqz_edge_woe b on a.edge = b.edge
group by a.order_src;

--合并最总结果
drop table overdue_result_all_new_woe;
create table overdue_result_all_new_woe as
select a.*,
b.edge_woe_sum,
b.edge_woe_max,
b.edge_woe_min,
c.depth
from overdue_result_all_new a 
left join fqz_order_edge_woe b on a.order_src = b.order_src
left join fqz_edge_depth c on a.order_src = c.order_src;

--数据类型转换
drop table overdue_result_all_new_woe_20170629;
insert overwrite table overdue_result_all_new_woe_20170629 as
select * from overdue_result_all_new_woe;
--=============================================
CREATE TABLE `overdue_result_all_new_woe_20170629`(
	  `label` double, 
	  `apply_time` string, 
	  `order_src` string, 
	  `order_cnt_self` double, 
	  `id_cnt_self` double, 
	  `black_cnt_self` double, 
	  `q_refuse_cnt_self` double, 
	  `pass_cnt_self` double, 
	  `overdue0_self` double, 
	  `overdue3_self` double, 
	  `overdue30_self` double, 
	  `overdue0_ls_self` double, 
	  `overdue3_ls_self` double, 
	  `overdue30_ls_self` double, 
	  `order_cnt` double, 
	  `id_cnt` double, 
	  `black_cnt` double, 
	  `q_refuse_cnt` double, 
	  `pass_cnt` double, 
	  `overdue0` double, 
	  `overdue3` double, 
	  `overdue30` double, 
	  `overdue0_ls` double, 
	  `overdue3_ls` double, 
	  `overdue30_ls` double, 
	  `order_cnt_2` double, 
	  `id_cnt_2` double, 
	  `black_cnt_2` double, 
	  `q_refuse_cnt_2` double, 
	  `pass_cnt_2` double, 
	  `overdue0_2` double, 
	  `overdue3_2` double, 
	  `overdue30_2` double, 
	  `overdue0_ls_2` double, 
	  `overdue3_ls_2` double, 
	  `overdue30_ls_2` double, 
	  `ljmx` string, 
	  `edge_woe_sum` double, 
	  `edge_woe_max` double, 
	  `edge_woe_min` double,
    `depth` double)


--================================================
create  table overdue_result_all_new_woe_20170630 as 
select 
label		
,apply_time		
,order_src		
,order_cnt_self		
,id_cnt_self		
,black_cnt_self		
,q_refuse_cnt_self		
,pass_cnt_self				
,overdue3_self		
,overdue30_self		
,overdue0_ls_self		
,overdue3_ls_self		
,overdue30_ls_self		
,order_cnt		
,id_cnt		
,black_cnt		
,q_refuse_cnt		
,pass_cnt				
,overdue3		
,overdue30		
,overdue0_ls		
,overdue3_ls		
,overdue30_ls		
,order_cnt_2		
,id_cnt_2		
,black_cnt_2		
,q_refuse_cnt_2		
,pass_cnt_2				
,overdue3_2		
,overdue30_2		
,overdue0_ls_2		
,overdue3_ls_2		
,overdue30_ls_2		
,ljmx		
,edge_woe_sum		
,edge_woe_max		
,edge_woe_min
from overdue_result_all_new_woe_20170629	;
--================================================
select
edge,
ln((good_cnt/12527)/(bad_cnt/29046)) as woe,
cnt,
cnt/41573 as cnt_percent,
bad_cnt,
bad_cnt/29046 as bad_cnt_percent,
good_cnt,
good_cnt/12527 as good_cnt_percent
from (
select edge,
count(*) as cnt,
sum(case when label = 0 then 1 else 0 end) as bad_cnt,
sum(case when label = 1 then 1 else 0 end) as good_cnt
from fqz_edge_data_total
group by edge) tab
where tab.edge = 'recommend|loginmobile|recommend|emergencymobile'
; 

--=======================================================================
--数据验证
select 
label		,
apply_time		,
order_src		,
order_cnt_self		,
id_cnt_self		,
q_refuse_cnt_self		,
pass_cnt_self		,
overdue0_self		,
overdue3_self		,
overdue30_self		,
overdue0_ls_self		,
overdue3_ls_self		,
overdue30_ls_self		,
order_cnt		,
id_cnt		,
q_refuse_cnt		,
pass_cnt		,
overdue0		,
overdue3		,
overdue30		,
overdue0_ls		,
overdue3_ls		,
overdue30_ls		,
order_cnt_2		,
id_cnt_2		,
q_refuse_cnt_2		,
pass_cnt_2		,
overdue0_2		,
overdue3_2		,
overdue30_2		,
overdue0_ls_2		,
overdue3_ls_2		,
overdue30_ls_2		
from overdue_result_all_new_woe_20170629  where order_src in ('SOA20170228232747000100005177548','TFA20161230064932000006','TFA20161229203222000021')
union all
select 
label		,
apply_time		,
order_src		,
order_cnt_self		,
id_cnt_self		,
q_refuse_cnt_self		,
pass_cnt_self		,
overdue0_self		,
overdue3_self		,
overdue30_self		,
overdue0_ls_self		,
overdue3_ls_self		,
overdue30_ls_self		,
order_cnt		,
id_cnt		,
q_refuse_cnt		,
pass_cnt		,
overdue0		,
overdue3		,
overdue30		,
overdue0_ls		,
overdue3_ls		,
overdue30_ls		,
order_cnt_2		,
id_cnt_2		,
q_refuse_cnt_2		,
pass_cnt_2		,
overdue0_2		,
overdue3_2		,
overdue30_2		,
overdue0_ls_2		,
overdue3_ls_2		,
overdue30_ls_2		
from overdue_result_all_new_20170621 where order_src in ('SOA20170228232747000100005177548','TFA20161230064932000006','TFA20161229203222000021');
