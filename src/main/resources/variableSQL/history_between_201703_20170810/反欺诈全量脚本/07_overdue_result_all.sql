use lkl_card_score;


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
drop table overdue_result_all_new_911;
create table overdue_result_all_new_911 as
select 
c.label,
c.apply_time,
a.*,b.ljmx
from overdue_result_all a
left join order_src_bian b on a.order_src=b.order_src
join fqz_order_performance_data_new c on a.order_src = c.order_id;

---去个重
insert overwrite table overdue_result_all_new_911 ---284890
select
label
,apply_time
,order_src
,order_cnt_self
,id_cnt_self
,black_cnt_self
,q_refuse_cnt_self
,pass_cnt_self
,overdue0_self
,	overdue3_self
,	overdue30_self
,	overdue0_ls_self
,	overdue3_ls_self
,	overdue30_ls_self
,	order_cnt
,	id_cnt
,	black_cnt
,	q_refuse_cnt
,	pass_cnt
,	overdue0
,  overdue3
,	overdue30
,	overdue0_ls
,	overdue3_ls
,	overdue30_ls
,	order_cnt_2
,	id_cnt_2
,	black_cnt_2
,	q_refuse_cnt_2
,	pass_cnt_2
,	overdue0_2
,	overdue3_2
,	overdue30_2
,	overdue0_ls_2
,	overdue3_ls_2
,	overdue30_ls_2
,	ljmx
from overdue_result_all_new_911
group by
label
,apply_time
,order_src
,order_cnt_self
,id_cnt_self
,black_cnt_self
,q_refuse_cnt_self
,pass_cnt_self
,overdue0_self
,	overdue3_self
,	overdue30_self
,	overdue0_ls_self
,	overdue3_ls_self
,	overdue30_ls_self
,	order_cnt
,	id_cnt
,	black_cnt
,	q_refuse_cnt
,	pass_cnt
,	overdue0
,  overdue3
,	overdue30
,	overdue0_ls
,	overdue3_ls
,	overdue30_ls
,	order_cnt_2
,	id_cnt_2
,	black_cnt_2
,	q_refuse_cnt_2
,	pass_cnt_2
,	overdue0_2
,	overdue3_2
,	overdue30_2
,	overdue0_ls_2
,	overdue3_ls_2
,	overdue30_ls_2
,	ljmx;