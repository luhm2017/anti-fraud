drop table overdue_cnt_1;
create table overdue_cnt_1 as 
-- 一度_订单数量	4	
SELECT a.order_src,'order_cnt' title, count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
group by  a.order_src    
union all 
-- 一度_ID数量	4	
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='1' and a.apply_time_src>a.apply_time1
group by a.order_src 
-- 一度_Q标拒绝数量	1	
union all 
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现  
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1
group by a.order_src 
union all
-- 一度_通过合同数量	2	
SELECT a.order_src,'pass_cnt' title,count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1
group by a.order_src ;

drop table  lkl_card_score.overdue_cnt_1_sum;
create table  lkl_card_score.overdue_cnt_1_sum  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1
group by order_src;

-- 一度_当前无逾期数量	1	   当前逾期天数为0天
-- 一度_当前3+数量	1	       当前逾期天数为3天以上
-- 一度_当前30+数量	1	       当前逾期天数30天以上
drop table overdue_cnt_2_tmp;
create table overdue_cnt_2_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src<='0' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src>'3' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src>'30' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1 
  group by c.order_src 
union all
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src<='0' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src>'3' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src>'30' 
   and c.degree_type='1' and c.apply_time_src>c.apply_time1  
   group by c.order_src
   ;
   


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

 
drop table overdue_result_1;
create table overdue_result_1 as  
select a.order_src,a.order_cnt,a.id_cnt,a.q_refuse_cnt,a.pass_cnt
       ,b.overdue0,b.overdue3,b.overdue30,b.overdue0_ls,b.overdue3_ls,b.overdue30_ls
from overdue_cnt_1_sum a 
join overdue_cnt_2 b on a.order_src=b.order_src;


 
-- 2度关联数据

drop table overdue_cnt_1_2;
create table overdue_cnt_1_2 as 
-- 一度_订单数量	4	
SELECT a.order_src,'order_cnt' title, count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='2' and a.apply_time_src>a.apply_time2
group by  a.order_src    
union all 
-- 一度_ID数量	4	
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no2) cnt FROM fqz_order_related_graph a     --order订单表现
where a.degree_type='2' and a.apply_time_src>a.apply_time2
group by a.order_src 
-- 一度_Q标拒绝数量	2	
union all 
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现  
where performance2='q_refuse' and a.degree_type='2' and a.apply_time_src>a.apply_time2
group by a.order_src 
union all
-- 一度_通过合同数量	2	
SELECT a.order_src,'pass_cnt' title,count(distinct a.order_src) cnt FROM fqz_order_related_graph a     --order订单表现
where a.type2='pass'  and a.degree_type='2' and a.apply_time_src>a.apply_time2
group by a.order_src 
;

drop table overdue_cnt_2_sum;
create table  lkl_card_score.overdue_cnt_2_sum  as
select order_src,
sum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           
sum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,   
sum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    
sum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt 
from  overdue_cnt_1_2
group by order_src;

-- 一度_当前无逾期数量	2	   当前逾期天数为0天
-- 一度_当前3+数量	2	       当前逾期天数为3天以上
-- 一度_当前30+数量	2	       当前逾期天数30天以上
drop table overdue_cnt_2_2_tmp;
create table overdue_cnt_2_2_tmp as 
select c.order_src,
        'overdue0' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src<='0' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
   group by c.order_src
union all
select c.order_src,
        'overdue3' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src>'3' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue30' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.current_due_day_src>'30' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1 
  group by c.order_src 
union all
select c.order_src,
        'overdue0_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src<='0' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue3_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src>'3' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
  group by c.order_src
union all
select c.order_src,
        'overdue30_ls' title
       ,count(distinct c.order_src) cnt 
       from 
   fqz_order_related_graph c 
 where c.type1='pass'       --通过 
   and c.history_due_day_src>'30' 
   and c.degree_type='2' and c.apply_time_src>c.apply_time1  
   group by c.order_src
   ;
   


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


drop table overdue_result_2;
create table overdue_result_2 as  
select a.order_src,a.order_cnt,a.id_cnt,a.q_refuse_cnt,a.pass_cnt
       ,b.overdue0,b.overdue3,b.overdue30,b.overdue0_ls,b.overdue3_ls,b.overdue30_ls
from overdue_cnt_2_sum a 
join overdue_cnt_2_2 b on a.order_src=b.order_src;

create table order_src_group as  
select trim(order_src) order_src   
from 
( select c0 as order_src from fqz_black_order_data1
union all
select c0 as order_src from   fqz_black_order_data2
) a   
group by trim(order_src); 

drop table overdue_result_all;
create table overdue_result_all as  
select c.order_src
       ,nvl(a.order_cnt,0) order_cnt
       ,nvl(a.id_cnt,0)  id_cnt
       ,nvl(a.q_refuse_cnt,0) q_refuse_cnt
       ,nvl(a.pass_cnt,0) pass_cnt
       ,nvl(a.overdue0,0)  overdue0
       ,nvl(a.overdue3,0) overdue3
       ,nvl(a.overdue30,0) overdue30
       ,nvl(a.overdue0_ls,0) overdue0_ls
       ,nvl(a.overdue3_ls,0) overdue3_ls
       ,nvl(a.overdue30_ls,0) overdue30_ls
       ,nvl(b.order_cnt ,0) order_cnt_2
       ,nvl(b.id_cnt ,0) id_cnt_2
       ,nvl(b.q_refuse_cnt ,0) q_refuse_cnt_2
       ,nvl(b.pass_cnt ,0) pass_cnt_2
       ,nvl(b.overdue0 ,0) overdue0_2
       ,nvl(b.overdue3 ,0)  overdue3_2
       ,nvl(b.overdue30 ,0) overdue30_2
       ,nvl(b.overdue0_ls ,0) overdue0_ls_2
       ,nvl(b.overdue3_ls ,0) overdue3_ls_2
       ,nvl(b.overdue30_ls,0) overdue30_ls_2
from order_src_group c 
left join overdue_result_1 a on c.order_src=a.order_src
left join overdue_result_2 b on c.order_src=b.order_src;


-----关联边 
---create table order_src_bian as 
---select c.order_src,concat_ws(',',ljmx)
---from (
---select a.order_src,
---       a.apply_time_src  --日期 
---       ,concat(case when a.c1='bankcard' then 'a' 
---            when a.c1='imei' then 'b' 
---            when a.c1='email' then 'c' 
---            when a.c1='mobile' then 'd' 
---            when a.c1='emergency' then 'e' 
---            when a.c1='contact' then 'f' 
---            when a.c1='empmobile' then 'g' 
---            when a.c1='company' then 'h' 
---            when a.c1='phone' then 'i' 
---            when a.c1='lbs' then 'j'  end,
---       case when a.c3='bankcard' then 'a' 
---            when a.c3='imei' then 'b' 
---            when a.c3='email' then 'c' 
---            when a.c3='mobile' then 'd' 
---            when a.c3='emergency' then 'e' 
---            when a.c3='contact' then 'f' 
---            when a.c3='empmobile' then 'g' 
---            when a.c3='company' then 'h' 
---            when a.c3='phone' then 'i' 
---            when a.c3='lbs' then 'j'  end,
---       case when a.c5='bankcard' then 'a' 
---            when a.c5='imei' then 'b'   
---            when a.c5='email' then 'c' 
---            when a.c5='mobile' then 'd' 
---            when a.c5='emergency' then 'e' 
---            when a.c5='contact' then 'f' 
---            when a.c5='empmobile' then 'g' 
---            when a.c5='company' then 'h' 
---            when a.c5='phone' then 'i' 
---            when a.c5='lbs' then 'j'  end,
---       case when a.c7='bankcard' then 'a' 
---            when a.c7='imei' then 'b' 
---            when a.c7='email' then 'c' 
---            when a.c7='mobile' then 'd' 
---            when a.c7='emergency' then 'e' 
---            when a.c7='contact' then 'f' 
---            when a.c7='empmobile' then 'g' 
---            when a.c7='company' then 'h' 
---            when a.c7='phone' then 'i' 
---            when a.c7='lbs' then 'j'  end) as ljmx
---           
---from  fqz_order_related_graph  a) c
---group by c.order_src;

-- 或者 

drop table order_src_bian_tmp;
create table order_src_bian_tmp as 
select 
order_src,
concat(c1,c3) as ljmx ,
case when performance1 = 'q_refuse' or history_due_day1 >90 then 1 else 0 end performance
from fqz_order_related_graph
where degree_type = '1'
union all 
select 
order_src,
concat(c1,c3,c5,c7) as ljmx ,
case when performance2 = 'q_refuse' or history_due_day2 >90 then 1 else 0 end performance
from fqz_order_related_graph
where degree_type = '2';

drop table order_src_bian;
create table order_src_bian as 
select c.order_src,concat_ws(',',collect_set(ljmx)) as ljmx           
from  order_src_bian_tmp  c
group by c.order_src;

--边字段 关联到结果表 
drop table overdue_result_all_2;
create table overdue_result_all_2 as 
select a.*,b.ljmx
from overdue_result_all a
left join order_src_bian b on a.order_src=b.order_src;
 
--日期字段 关联到结果表  
drop table overdue_result_all_new;
create table overdue_result_all_new as 
select a.*,b.apply_time
from overdue_result_all_2 a
left join fqz_order_performance_data_new b on a.order_src=b.order_id;
