use lkl_card_score;

--һ�ȹ�������
drop table overdue_cnt_self;
create table overdue_cnt_self as 
-- һ�ȹ�������_��������     
SELECT a.order_src,'order_cnt' title, 
count(distinct a.order1) cnt 
FROM fqz_order_related_graph a     --order��������
where a.degree_type='1' 
and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by  a.order_src    
union all 
-- һ�ȹ�������_ID����  
SELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_related_graph a     --order��������
where a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src 
union all
-- һ�ȹ�������_�ں�ͬ����
SELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order��������
where a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1 and a.label1 = 1
group by a.order_src
-- һ�ȹ�������_Q��ܾ�����  1  
union all 
SELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order��������  
where performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src 
union all
-- һ�ȹ�������_ͨ����ͬ���� 
SELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_related_graph a     --order��������
where a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1
and a.cert_no_src = a.cert_no1
group by a.order_src ;

--�ϲ�����  һ�ȹ�������
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