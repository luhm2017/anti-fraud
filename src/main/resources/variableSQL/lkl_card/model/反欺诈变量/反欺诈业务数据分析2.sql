--信贷合同表现情况
--===============================================================
 0 -> 1  到期日逾期总额/到期信贷总额
 1 -> 2  t-1天逾期合同数（到期未还）/t-2逾期合同数

1、黑名单数据分析
关联总数  命中总数 3t逾期数	30t逾期数 60t逾期数 90逾期数 以及分别逾期占比率
2、逾期率滚动率
--===================================================
--查询每个合同（未还款）最大逾期天数
select a.contract_no, trunc(sysdate) - min(r.due_time) from c_loan_apply  a 
inner join c_apply_user u on u.id = a.id
inner join c_loan_contract c on c.apply_id = a.id 
inner join c_receipt t on t.contract_id = c.id 
inner join c_loan_receivable r on r.receipt_id = t.id
group by a.contract_no ;

--查询每个合同每期未还款逾期天数
select a.receipt_id,
a.cur_peroid,
trunc(sysdate)-trunc(a.due_time)
 from c_loan_receivable a
where a.capital_type = '0'
order by a.receipt_id,a.cur_peroid ;

--查询每期合同已还款逾期表现
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
--y值表现  提出无效合同 
--1、获取走完表现期的合同（至少走完3期，所以最迟2017.1.10）
--2、统计逾期滚动率
--3、定观察期
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
and a.due_time < '2017-04-12' --只考虑逾期数据
and a.year = '2017' 
and a.month = '04' 
and a.day = '10' 
union all
--到期已还的贷款（逾期）
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
) tab  --还款日期大于到期截止日即逾期
on tab.receipt_id = t.id
where a.year = '2017' and a.month = '04' and a.day = '10'
and t.year = '2017' and t.month = '04' and t.day = '10'
and a.status <> 'F'
and a.start_date <= '2017-01-10' --走完3期表现
and substr(a.product_no,1,2) in ('PC','PK')  --易分期产品
order by a.contract_no,tab.receipt_id,tab.cur_peroid;

--一度关联黑名单数据
--===========================================================================================
--导入数据
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


--反欺诈LBS分析
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
--反欺诈风控属性加工
--统计Q标、非Q标拒绝的占比
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

--统计信贷合同表现
--统计最大逾期天数，可以直接使用creditloan.r_overdue_period
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
private String cert_no;//身份证号
    private String apply_id;//申请id
    private String order_id;//订单号
    private String contract_no;//合同编码
    private String receipt_id;//借据号
    private String apply_time;//申请时间
    private String is_settle;//是否结清 1结清 0未结清
    private String business_no;//业务类型
    private String end_type;//G:提前逾期结束,C:提前结束,M:正常结束,E:正常逾期结束,2:异常结束
    private String cur_period;//统计期数
    private String due_time;//
    private String stat_count;//统计数量
    private String overdue_type;//stat_type是0 则0表示未还清 1表示已还清  stat_type是1 表示还款金额
    private String stat_type;//0天数 1金额
    private String stat_time;//统计时间
--=============================================================	


CREATE TABLE `r_ca_overdue` (
  `contract_no` varchar(48) DEFAULT NULL COMMENT '合同编码',
  `receipt_id` varchar(48) DEFAULT NULL COMMENT '借据号',
  `overdue_type` int(11) DEFAULT NULL COMMENT '1-已还，0-未还',
  `stat_type` int(11) DEFAULT NULL COMMENT '1-逾期金额，0逾期天数',
  `stat_count` int(11) DEFAULT NULL COMMENT '统计数量',
  `stat_time` date NOT NULL COMMENT '统计时间',
  `cur_period` int(11) DEFAULT NULL COMMENT '统计期数',
  `apply_id` varchar(16) DEFAULT NULL COMMENT '申请id',
  `order_id` varchar(48) DEFAULT NULL COMMENT '订单号',
  `cert_no` varchar(48) DEFAULT NULL COMMENT '身份证号',
  `apply_time` varchar(30) DEFAULT '' COMMENT '申请时间',
  `cpcr_codes` varchar(50) DEFAULT NULL COMMENT '单位地址省市区编码',
  `comp_addr` varchar(100) DEFAULT NULL COMMENT '单位地址',
  `comp_phone` varchar(50) DEFAULT NULL COMMENT '单位电话',
  `company` varchar(100) DEFAULT NULL COMMENT '公司名',
  `is_settle` varchar(2) DEFAULT NULL COMMENT '是否结清 1结清 0未结清',
  `business_no` varchar(10) DEFAULT NULL COMMENT '业务类型',
  `due_time` varchar(30) DEFAULT NULL,
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号',
  KEY `idx_ca_overdue_certno` (`cert_no`),
  KEY `idx_ca_overdue_contractno` (`contract_no`),
  KEY `idx_mobile` (`mobile`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='逾期变量表';

--======================================================================
--order订单表现
--13539520
--除重 13539520 
--订单合同表现数据
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
r.overdue_type --0表示未还清 1表示已还清
from (
select 
a.order_id, --订单
max(a.stat_count) as performance, --最大值
a.apply_time, --申请时间
a.overdue_type  --是否还清
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type
) r;

--关联身份证号码
create table fqz_order_performance_data_new_1 as          --	13539503
select f.*,u.cert_no from fqz_order_performance_data f    --	13539520 
join creditloan.s_c_loan_apply  a on trim(f.order_id) = trim(a.order_id)
join creditloan.s_c_apply_user u on u.id = a.id
where a.year = 2017 and a.month = 05 and a.day = 31
and u.year = 2017 and u.month = 05 and u.day = 31;


--合同表现
--=======================================================
create table faq_order_performance_data as 
select 
a.order_id, --订单
max(a.stat_count) as performance, --最大值
a.apply_time, --申请时间
a.overdue_type  --是否还清
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type;
--==========================================================
--黑合同标注边权重统计（q_refuse、other_refuse、90+）
--黑合同数据
select count(*) from fqz_black_order_data1;  --2276831
select count(*) from fqz_black_order_data2;  --115245246

--黑合同一二度关联数据
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

--黑合同一二度关联边统计
--==========================================
--=====================一度边权重
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

--=========================二度边权重
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
--黑名单关联数据
fqz_black_related_data0   --4758862
fqz_black_related_data1   --6827339
fqz_black_related_data2   --34927352


--黑名单关联数据（0、1、2度）
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

--多度关联数据(订单表现)
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
--统计多度关联边权重
--refuse 41808457
--0度关联统计
select 
a.c1 as edge0,
count(a.apply0) as cnt,
sum(case when a.apply0_perform = 'q_refuse' then 1 else 0 end) as bad_cnt
from fqz_black_related_graph_performance a
where a.degreetype = '0'
and a.apply0_type = 'refuse'
group by a.c1;

--1度关联统计
select 
a.c3,
a.c5,
count(a.apply1) as cnt,
sum(case when a.apply1_perform = 'q_refuse' then 1 else 0 end) as bad_cnt
from fqz_black_related_graph_performance a
where a.degreetype = '1'
and a.apply0_type = 'refuse'
group by a.c3,a.c5;


--2度关联统计
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

--=====================================关联边统计
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

--统计0度关联
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
 
 --统计1度关联
 --================================================================
 select 
a.edge1,
sum(a.performance2) as cnt,
sum(case when performance2 = 't_bad' or performance2 = 'f_bad' then 1 else 0 end) as bad_cnt
 from fqz_black_related_graph a
where (performance1 = 't_bad' or performance1 = 'f_bad');

--统计2度关联
--================================================================
select 
a.edge1,
a.edge2,
sum(a.performance3) as cnt,
sum(case when performance3 = 't_bad' or performance3 = 'f_bad' then 1 else 0 end) as bad_cnt
 from fqz_black_related_graph a
where (performance1 = 't_bad' or performance1 = 'f_bad');


--0度关联
--==================================================================================
--1度关联
--==================================================================================
match(n:Mobile)-[r:loanapply]-(m:ApplyInfo)-[rr:loanapply]-(nn:Mobile)-[rrr:applymymobile]-(mm:ApplyInfo) where n.content = '13358654987' return n.content,m.orderno,nn.content,mm.orderno
--2度关联
--==================================================================================




--风控规则梳理
--===================================================================================
进件申请渠道 --进件属性
进件申请时间 --进件属性
单位名称
单位地址
单位电话
申请人手机号
紧急联系人手机号
亲属人手机号
人行手机号
人行配偶手机号
身份证号码  --属性（申请人姓名）
身份证地址
个人住址
家庭电话
进件审批结果（拒绝代码） --进件属性
客户经理
推荐人
邮箱
IMEI   --手机型号（安卓、IOS）
银行卡号
终端号
IPV4
LBS
设备指纹
人行规则



[\"进件\",\"手机号\",\"设备\",\"银行卡\",\"身份证\",\"Email\",\"终端\",\"住址\",
\"手机IMEI\",\"LBS\",\"公司名称\",\"固定电话\",\"公司地址\",\"IP地址\",\"公司电话\"];

进件
手机号
手机IMEI
银行卡
身份证
终端
Email
公司名称
固定电话
公司地址
IP地址
公司电话

----============================================
1、信贷渠道入口有哪些？app、h5、winxin
2、信贷主要产品，对应客群分布以及特征
3、信贷进件申请主要要素，（活检、身份证、银行卡、手机、申请拒绝原因等）
4、订单网关校验之后形成申请信息表，其中校验的主要内容
5、信贷审核主要流程：
	1）、央行征信
	2）、外部数据源黑名单
	3）、内部历史进件表现
	4）、金额、利率
6、合同生成后，如何放款？
7、逾期后如何处理预警
8、包括催收、上报征信
9、合同如何核销、处理

--==============================================================
--订单合同表现数据
create table faq_order_performance_data as 
select 
a.order_id, --订单
max(a.stat_count) as performance, --最大值
a.apply_time, --申请时间
a.overdue_type  --是否还清
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
r.overdue_type --0表示未还清 1表示已还清
from (
select 
a.order_id, --订单
max(a.stat_count) as performance, --最大值
a.apply_time, --申请时间
a.overdue_type  --是否还清
from 
creditloan.r_overdue_period a
where a.year = 2017 and a.month = 05 and a.day = 31
and a.stat_type = 0
group by order_id,apply_time,overdue_type
) r;

