--�Ŵ���ͬ�������
--===============================================================
 0 -> 1  �����������ܶ�/�����Ŵ��ܶ�
 1 -> 2  t-1�����ں�ͬ��������δ����/t-2���ں�ͬ��

1�����������ݷ���
��������  �������� 3t������	30t������ 60t������ 90������ �Լ��ֱ�����ռ����
2�������ʹ�����
--===================================================
--��ѯÿ����ͬ��δ��������������
select a.contract_no, trunc(sysdate) - min(r.due_time) from c_loan_apply  a 
inner join c_apply_user u on u.id = a.id
inner join c_loan_contract c on c.apply_id = a.id 
inner join c_receipt t on t.contract_id = c.id 
inner join c_loan_receivable r on r.receipt_id = t.id
group by a.contract_no ;

--��ѯÿ����ͬÿ��δ������������
select a.receipt_id,
a.cur_peroid,
trunc(sysdate)-trunc(a.due_time)
 from c_loan_receivable a
where a.capital_type = '0'
order by a.receipt_id,a.cur_peroid ;

--��ѯÿ�ں�ͬ�ѻ������ڱ���
select 
b.receipt_id,
b.cur_peroid,
trunc(b.RECEIPT_TIME)-trunc(b.DUE_TIME)
 from c_loan_repay b
where b.capital_type = '0'
and b.RECEIPT_TIME is not null
and b.DUE_TIME is not null
and trunc(b.RECEIPT_TIME) >= b.DUE_TIME
order by b.receipt_id,b.cur_peroid;

--===================================================
--yֵ����  �����Ч��ͬ 
--1����ȡ��������ڵĺ�ͬ����������3�ڣ��������2017.1.10��
--2��ͳ�����ڹ�����
--3�����۲���
-- 
create table fqz_contract_no_yfq_performance as
select a.contract_no,a.start_date,tab.receipt_id,tab.cur_peroid,
tab.capital_amount,tab.overdue_day
from creditloan.s_c_loan_contract a 
join r_creditloan.s_c_receipt t on t.contract_id = a.id 
join 
(
select  
a.receipt_id,
a.cur_peroid,
a.receivable as capital_amount,
datediff('2017-04-12',a.due_time) as overdue_day
from r_creditloan.s_c_loan_receivable a 
where a.capital_type = '0'
and a.due_time < '2017-04-12' --ֻ������������
and a.year = '2017' 
and a.month = '04' 
and a.day = '10' 
union all
--�����ѻ��Ĵ�����ڣ�
select 
b.receipt_id,
b.cur_peroid,
b.receipt as capital_amount,
datediff(b.receipt_time , b.due_time) as overdue_day 
from r_creditloan.s_c_loan_repay b
where b.capital_type = '0'
and substr(b.receipt_time,1,7) > substr(b.due_time,1,7)
and b.year = '2017' 
and b.month = '04' 
and b.day = '10'
) tab  --�������ڴ��ڵ��ڽ�ֹ�ռ�����
on tab.receipt_id = t.id
where a.year = '2017' and a.month = '04' and a.day = '10'
and t.year = '2017' and t.month = '04' and t.day = '10'
and a.status <> 'F'
and a.start_date <= '2017-01-10' --����3�ڱ���
and substr(a.product_no,1,2) in ('PC','PK')  --�׷��ڲ�Ʒ
order by a.contract_no,tab.receipt_id,tab.cur_peroid;

--һ�ȹ�������������
--===========================================================================================
--��������
 create table lkl_card_score_credit_contract(contract_no string, gbie double)
 row format delimited fields terminated by ',' ;
 load data  inpath '/tmp/XJD_GBIE.csv' overwrite into table lkl_card_score_credit_contract;
 
 create  table fqz_black_data (
  blackmobile string,
  orderno string,
  mobile string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
load data inpath '/user/luhuamin/data.csv' overwrite into table fqz_black_data;


--����թLBS����
--===========================================================
select count(*) from creditloan.s_data_generalapply_creditloan  
where year= 2017 
and month=04 
and day=03  
and CHANNELSOURCE='A' 
and ( lbs is null or lbs='')

select  `_platform`,lbs from  r_creditloan.s_data_generalapply_creditloan  
where year= 2017 and month=04 and day=06 
and CHANNELSOURCE='A' 
and (length(get_json_object(lbs,'$.lbsanalyzed')) <5 or lbs='' or lbs is null)


--===========================================================
--����թ������Լӹ�
--ͳ��Q�ꡢ��Q��ܾ���ռ��
create table fqz_refuse_data as 
select 
ORDER_ID,
PAN_AUTH_STATUS,
MANUAL_AUTH_STATUS,
FAIL_REASON,
STATUS,
AUDIT_TIME
from creditloan.s_c_loan_apply a
where a.status = 'F'
and a.year = 2017 
and a.month = 05 
and day = 08;

--ͳ���Ŵ���ͬ����
--ͳ�������������������ֱ��ʹ��creditloan.r_overdue_period
--================================================================
select
 a.order_id,
 t.out_range
from creditloan.s_c_loan_apply  a 
join creditloan.s_c_apply_user u on u.id = a.id
join creditloan.s_c_loan_contract c on c.apply_id = a.id 
join creditloan.s_c_receipt t on t.contract_id = c.id 
join creditloan.s_c_loan_receivable r on r.receipt_id = t.id
where a.year = 2017 and a.month = 05 and a.day = 08
and u.year = 2017 and u.month = 05 and u.day = 08
and c.year = 2017 and c.month = 05 and c.day = 08
and t.year = 2017 and t.month = 05 and t.day = 08
and r.year = 2017 and r.month = 05 and r.day = 08;


select * from creditloan.r_overdue_period 
where year = 2017 and month = 05 and day = 08
limit 11; 
--===========================================================
private String cert_no;//���֤��
    private String apply_id;//����id
    private String order_id;//������
    private String contract_no;//��ͬ����
    private String receipt_id;//��ݺ�
    private String apply_time;//����ʱ��
    private String is_settle;//�Ƿ���� 1���� 0δ����
    private String business_no;//ҵ������
    private String end_type;//G:��ǰ���ڽ���,C:��ǰ����,M:��������,E:�������ڽ���,2:�쳣����
    private String cur_period;//ͳ������
    private String due_time;//
    private String stat_count;//ͳ������
    private String overdue_type;//stat_type��0 ��0��ʾδ���� 1��ʾ�ѻ���  stat_type��1 ��ʾ������
    private String stat_type;//0���� 1���
    private String stat_time;//ͳ��ʱ��
--=============================================================	


CREATE TABLE `r_ca_overdue` (
  `contract_no` varchar(48) DEFAULT NULL COMMENT '��ͬ����',
  `receipt_id` varchar(48) DEFAULT NULL COMMENT '��ݺ�',
  `overdue_type` int(11) DEFAULT NULL COMMENT '1-�ѻ���0-δ��',
  `stat_type` int(11) DEFAULT NULL COMMENT '1-���ڽ�0��������',
  `stat_count` int(11) DEFAULT NULL COMMENT 'ͳ������',
  `stat_time` date NOT NULL COMMENT 'ͳ��ʱ��',
  `cur_period` int(11) DEFAULT NULL COMMENT 'ͳ������',
  `apply_id` varchar(16) DEFAULT NULL COMMENT '����id',
  `order_id` varchar(48) DEFAULT NULL COMMENT '������',
  `cert_no` varchar(48) DEFAULT NULL COMMENT '���֤��',
  `apply_time` varchar(30) DEFAULT '' COMMENT '����ʱ��',
  `cpcr_codes` varchar(50) DEFAULT NULL COMMENT '��λ��ַʡ��������',
  `comp_addr` varchar(100) DEFAULT NULL COMMENT '��λ��ַ',
  `comp_phone` varchar(50) DEFAULT NULL COMMENT '��λ�绰',
  `company` varchar(100) DEFAULT NULL COMMENT '��˾��',
  `is_settle` varchar(2) DEFAULT NULL COMMENT '�Ƿ���� 1���� 0δ����',
  `business_no` varchar(10) DEFAULT NULL COMMENT 'ҵ������',
  `due_time` varchar(30) DEFAULT NULL,
  `mobile` varchar(32) DEFAULT NULL COMMENT '�ֻ���',
  KEY `idx_ca_overdue_certno` (`cert_no`),
  KEY `idx_ca_overdue_contractno` (`contract_no`),
  KEY `idx_mobile` (`mobile`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='���ڱ�����';

--======================================================================
--order��������
--13539520
--���� 13539520 
--������ͬ��������
create table fqz_order_performance_data as 
select 
ORDER_ID,
case when FAIL_REASON like '%Q%' THEN 'q_refuse' ELSE 'other_refuse' end as performance,
LOAN_DATE as apply_time,
'refuse' as type,
'null' as  overdue_type
from creditloan.s_c_loan_apply a
where a.status = 'F'
and a.year = 2017 
and a.month = 05 
and day = 31
union all
select
r.order_id,
r.performance, 
r.apply_time,
'pass' as type,
r.overdue_type --0��ʾδ���� 1��ʾ�ѻ���
from (
select 
a.order_id, --����
max(a.stat_count) as performance, --���ֵ
a.apply_time, --����ʱ��
a.overdue_type  --�Ƿ���
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type
) r;

--�������֤����
create table fqz_order_performance_data_new_1 as          --	13539503
select f.*,u.cert_no from fqz_order_performance_data f    --	13539520 
join creditloan.s_c_loan_apply  a on trim(f.order_id) = trim(a.order_id)
join creditloan.s_c_apply_user u on u.id = a.id
where a.year = 2017 and a.month = 05 and a.day = 31
and u.year = 2017 and u.month = 05 and u.day = 31;


--��ͬ����
--=======================================================
create table faq_order_performance_data as 
select 
a.order_id, --����
max(a.stat_count) as performance, --���ֵ
a.apply_time, --����ʱ��
a.overdue_type  --�Ƿ���
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type;
--==========================================================
--�ں�ͬ��ע��Ȩ��ͳ�ƣ�q_refuse��other_refuse��90+��
--�ں�ͬ����
select count(*) from fqz_black_order_data1;  --2276831
select count(*) from fqz_black_order_data2;  --115245246

--�ں�ͬһ���ȹ�������
--===========================================
create table fqz_order_related_graph as
select
'1' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.overdue_type as overdue_type_src,
b.cert_no as cert_no_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.overdue_type as overdue_type1,
c.cert_no as cert_no1,
'null' as c5,
'null' as c6,
'null' as c7,
'null' as order2,
'null' as performance2,
'null' as apply_time2,
'null' as type2,
'null' as overdue_type2,
'null' as cert_no2
from fqz_black_order_data1 a 
join fqz_order_performance_data_new b on a.c0 = b.order_id
join fqz_order_performance_data_new c on a.c4 = c.order_id
union all
select 
'2' as degree_type,
a.c0 as order_src,
b.performance as performance_src,
b.apply_time as apply_time_src,
b.type as type_src,
b.overdue_type as overdue_type_src,
b.cert_no as cert_no_src,
a.c1,a.c2,a.c3,
a.c4 as order1,
c.performance as performance1,
c.apply_time as apply_time1,
c.type as type1,
c.overdue_type as overdue_type1,
c.cert_no as cert_no1,
a.c5,a.c6,a.c7,
a.c8 as order2,
d.performance as performance2,
d.apply_time as apply_time2,
d.type as type2,
d.overdue_type as overdue_type2,
d.cert_no as cert_no2
from fqz_black_order_data2 a 
join fqz_order_performance_data_new b on a.c0 = b.order_id
join fqz_order_performance_data_new c on a.c4 = c.order_id
join fqz_order_performance_data_new d on a.c8 = d.order_id;

--�ں�ͬһ���ȹ�����ͳ��
--==========================================
--=====================һ�ȱ�Ȩ��
select 
tab.c1,
tab.c3,
count(tab.order1) as cnt,
sum(case when tab.performance1 = 'q_refuse' or tab.performance1 > 90 then 1 else 0 end) as bad_cnt,
sum(case when tab.performance1 = 'q_refuse' then 1 else 0 end) as q_refuse_cnt,
sum(case when tab.performance1 > 90 then 1 else 0 end) as due_over_90,
sum(case when tab.performance1 > 60 then 1 else 0 end) as due_over_60,
sum(case when tab.performance1 > 30 then 1 else 0 end) as due_over_30,
sum(case when tab.performance1 > 3 then 1 else 0 end) as due_over_3
from 
(
select
a.order_src,
a.performance_src,
a.c1,
a.c3,
a.order1,
max(a.performance1) as performance1
from fqz_order_related_graph a
where a.degree_type = '1' and a.type1 = 'pass'
group by order_src,performance_src,order1,a.c1,a.c3
union all
select
a.order_src,
a.performance_src,
a.c1,
a.c3,
a.order1,
a.performance1
from fqz_order_related_graph a
where a.degree_type = '1' and a.type1 = 'refuse'
) tab
group by c1,c3;

--=========================���ȱ�Ȩ��
select 
tab.c1,
tab.c3,
tab.c5,
tab.c7,
count(tab.order2) as cnt,
sum(case when tab.performance2 = 'q_refuse' or tab.performance2 > 90 then 1 else 0 end) as bad_cnt
from 
(
select
a.order_src,
a.performance_src,
a.c1,
a.c3,
a.c5,
a.c7,
a.order2,
max(a.performance2) as performance2
from fqz_order_related_graph a
where a.degree_type = '2' and a.type2 = 'pass'
group by order_src,performance_src,order2,a.c1,a.c3,a.c5,a.c7
union all
select
a.order_src,
a.performance_src,
a.c1,
a.c3,
a.c5,
a.c7,
a.order2,
a.performance2
from fqz_order_related_graph a
where a.degree_type = '2' and a.type2 = 'refuse'
) tab
group by c1,c3,c5,c7;

--==============================================================
--��������������
fqz_black_related_data0   --4758862
fqz_black_related_data1   --6827339
fqz_black_related_data2   --34927352


--�������������ݣ�0��1��2�ȣ�
create table fqz_black_related_graph as
select 
'0' as degreetype,
a.tagging_edge as c0,
a.edge0 as c1,
a.apply0 as c2,
'null' as c3,
'null' as c4,
'null' as c5,
'null' as c6,
'null' as c7,
'null' as c8,
'null' as c9,
'null' as c10
from fqz_black_related_data0 a
union all 
select 
'1' as degreetype,
c0,
c1,
c2,
c3,
c4,
c5,
c6,
'null' as c7,
'null' as c8,
'null' as c9,
'null' as c10
from fqz_black_related_data1
union all 
select 
'2' as degreetype,
c0,
c1,
c2,
c3,
c4,
c5,
c6,
c7,
c8,
c9,
c10
from fqz_black_related_data2;

--��ȹ�������(��������)
--======================================
create table fqz_black_related_graph_performance as 
select  
a.degreetype	   ,
a.c0			   ,
a.c1			   ,
a.c2	as apply0  ,
b.performance as apply0_perform,
b.apply_time as apply0_apply_time,
b.type as apply0_type,
b.settle_type as apply0_settle_type,
a.c3			   ,
a.c4			   ,
a.c5			   ,
a.c6	as apply1  ,
c.performance as apply1_perform,
c.apply_time as apply1_apply_time,
c.type as apply1_type,
c.settle_type as apply1_settle_type,
a.c7			 ,
a.c8			 ,
a.c9			 ,
a.c10	as apply2,
d.performance as apply2_perform,
d.apply_time as apply2_apply_time,
d.type as apply2_type,
d.settle_type as apply2_settle_type
from fqz_black_related_graph a 
left join fqz_refuse_and_performance_data b on a.c2 = b.order_id
left join fqz_refuse_and_performance_data c on a.c6 = c.order_id
left join fqz_refuse_and_performance_data d on a.c10 = d.order_id;

---==============================================
--ͳ�ƶ�ȹ�����Ȩ��
--refuse 41808457
--0�ȹ���ͳ��
select 
a.c1 as edge0,
count(a.apply0) as cnt,
sum(case when a.apply0_perform = 'q_refuse' then 1 else 0 end) as bad_cnt
from fqz_black_related_graph_performance a
where a.degreetype = '0'
and a.apply0_type = 'refuse'
group by a.c1;

--1�ȹ���ͳ��
select 
a.c3,
a.c5,
count(a.apply1) as cnt,
sum(case when a.apply1_perform = 'q_refuse' then 1 else 0 end) as bad_cnt
from fqz_black_related_graph_performance a
where a.degreetype = '1'
and a.apply0_type = 'refuse'
group by a.c3,a.c5;


--2�ȹ���ͳ��
select 
a.c3,
a.c5,
a.c7,
a.c9,
count(distinct a.apply2) as cnt,
sum(case when a.apply2_perform = 'q_refuse' then 1 else 0 end) as bad_cnt
from fqz_black_related_graph_performance a
where a.degreetype = '2'
and a.apply0_type = 'refuse'
group by a.c3,a.c5,a.c7,a.c9;

--=====================================������ͳ��
create table fqz_black_related_graph as 
select 
a.degreetype,
a.tagging_edge ,
a.edge0,
a.apply1 ,
b.performance, 
a.edge1  ,
a.apply2 ,
c.performance,
a.edge2  ,
a.apply3 ,
d.performance 
 from fqz_black_related_data a 
join fqz_refuse_and_performance_data b on a.order_id = b.order_id
join fqz_refuse_and_performance_data c on a.order_id = c.order_id
join fqz_refuse_and_performance_data d on a.order_id = d.order_id;

--ͳ��0�ȹ���
--================================================================
create table fqz_black_related_graph0 as 
select 
a.degreetype,  --
a.tagging_edge ,
a.edge0 ,
a.apply0 as order_id,
b.performance,
b.apply_time 
 from fqz_black_related_data0 a 
join fqz_refuse_and_performance_data b on a.apply0 = b.order_id;
 
 --ͳ��1�ȹ���
 --================================================================
 select 
a.edge1,
sum(a.performance2) as cnt,
sum(case when performance2 = 't_bad' or performance2 = 'f_bad' then 1 else 0 end) as bad_cnt
 from fqz_black_related_graph a
where (performance1 = 't_bad' or performance1 = 'f_bad');

--ͳ��2�ȹ���
--================================================================
select 
a.edge1,
a.edge2,
sum(a.performance3) as cnt,
sum(case when performance3 = 't_bad' or performance3 = 'f_bad' then 1 else 0 end) as bad_cnt
 from fqz_black_related_graph a
where (performance1 = 't_bad' or performance1 = 'f_bad');


--0�ȹ���
--==================================================================================
--1�ȹ���
--==================================================================================
match(n:Mobile)-[r:loanapply]-(m:ApplyInfo)-[rr:loanapply]-(nn:Mobile)-[rrr:applymymobile]-(mm:ApplyInfo) where n.content = '13358654987' return n.content,m.orderno,nn.content,mm.orderno
--2�ȹ���
--==================================================================================




--��ع�������
--===================================================================================
������������ --��������
��������ʱ�� --��������
��λ����
��λ��ַ
��λ�绰
�������ֻ���
������ϵ���ֻ���
�������ֻ���
�����ֻ���
������ż�ֻ���
���֤����  --���ԣ�������������
���֤��ַ
����סַ
��ͥ�绰
��������������ܾ����룩 --��������
�ͻ�����
�Ƽ���
����
IMEI   --�ֻ��ͺţ���׿��IOS��
���п���
�ն˺�
IPV4
LBS
�豸ָ��
���й���



[\"����\",\"�ֻ���\",\"�豸\",\"���п�\",\"���֤\",\"Email\",\"�ն�\",\"סַ\",
\"�ֻ�IMEI\",\"LBS\",\"��˾����\",\"�̶��绰\",\"��˾��ַ\",\"IP��ַ\",\"��˾�绰\"];

����
�ֻ���
�ֻ�IMEI
���п�
���֤
�ն�
Email
��˾����
�̶��绰
��˾��ַ
IP��ַ
��˾�绰

----============================================
1���Ŵ������������Щ��app��h5��winxin
2���Ŵ���Ҫ��Ʒ����Ӧ��Ⱥ�ֲ��Լ�����
3���Ŵ�����������ҪҪ�أ�����졢���֤�����п����ֻ�������ܾ�ԭ��ȣ�
4����������У��֮���γ�������Ϣ������У�����Ҫ����
5���Ŵ������Ҫ���̣�
	1������������
	2�����ⲿ����Դ������
	3�����ڲ���ʷ��������
	4����������
6����ͬ���ɺ���ηſ
7�����ں���δ���Ԥ��
8���������ա��ϱ�����
9����ͬ��κ���������

--==============================================================
--������ͬ��������
create table faq_order_performance_data as 
select 
a.order_id, --����
max(a.stat_count) as performance, --���ֵ
a.apply_time, --����ʱ��
a.overdue_type  --�Ƿ���
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type;

create table fqz_order_performance_data as 
select 
ORDER_ID,
case when FAIL_REASON like '%Q%' THEN 'q_refuse' ELSE 'other_refuse' end as performance,
LOAN_DATE as apply_time,
'refuse' as type,
'null' as  overdue_type
from creditloan.s_c_loan_apply a
where a.status = 'F'
and a.year = 2017 
and a.month = 05 
and day = 31
union all
select
r.order_id,
r.performance, 
r.apply_time,
'pass' as type,
r.overdue_type --0��ʾδ���� 1��ʾ�ѻ���
from (
select 
a.order_id, --����
max(a.stat_count) as performance, --���ֵ
a.apply_time, --����ʱ��
a.overdue_type  --�Ƿ���
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type
) r;

