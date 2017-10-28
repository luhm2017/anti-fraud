--近12个月信用卡还款临时表
set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;
set mapreduce.reduce.shuffle.input.buffer.percent=0.6;

use lkl_card_score;

 
 
--近3个月信用卡还款临时表
drop table tmp_3month_hk;
create table tmp_3month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=90
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
  and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);
 
 
 --近6个月信用卡还款临时表
drop table tmp_6month_hk;
create table tmp_6month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=180
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
  and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);
 
 
  --近18个月信用卡还款临时表
  drop table tmp_18month_hk;
create table tmp_18month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=540
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
  and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);

drop table tmp_12month_hk;
create table lkl_card_score.tmp_12month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
left join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=365  
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
 and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825); 
 
--近24个月信用卡还款临时表
drop table tmp_24month_hk;
create table tmp_24month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=730
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
  and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);

--近36个月信用卡还款临时表
drop table tmp_36month_hk;
create table tmp_36month_hk as
SELECT   t.contact              --手机号 
        ,t.endtime           --交易时间
        ,t.acctno               --银行账号
        ,t.amount
        ,c.bankname             --银行名称
        ,g.acctype              --账户种类  
        ,d.PAY_TIME
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lmps.s_tbl_cardbininfo c on c.accountbin=substr(t.accountbin,1,6) and c.year='2016' and c.month='12' and c.day='01'  --kabin关联
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where b.buscode = 'creditCardRepayments'   --信用卡还款类
 and  datediff(d.PAY_TIME,t.endtime)<=1095
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
  and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);
 

--历史信用卡还款临时表
drop table tmp_allmonth_hk;
create table tmp_allmonth_hk as
SELECT  t.contact as phone,    --手机号 
        t.endtime 
        ,t.acctno               --银行账号
        ,g.acctype              --账户种类
        ,d.pay_time
from r_lmps.s_tbl_tradelog  t
left join r_lmps.s_tbl_tradelist g on t.sid=g.sid
join lmps.customer_identifier_tradetype_base b on t.busid = b.busid
join lkl_card_score.phone_user_yfq d on d.mobile = t.contact          --信贷合同放款日期
where g.acctype='C'         --信用卡
 and b.buscode = 'creditCardRepayments'  --信用卡还款类
 and t.retcode = '00' 
 and t.amount <> 'null'
 and t.contact <> 'null'
 and t.endtime < d.pay_time
 and t.endtime > date_sub(d.pay_time,1825);
 


--所有变量UNION ALL成一个宽表 	
--历史信用卡最早一次还款时间距今时长	月份
 
 
drop table  lkl_card_score.all_title_table;
CREATE TABLE lkl_card_score.all_title_table(
	  contact  string, 
	  title string, 
	  values  string);

insert into  lkl_card_score.all_title_table	  
SELECT   phone as contact  ,'min_hkmonth' title, 
        datediff(PAY_TIME,min(endtime)) as values  --最早一次
from tmp_allmonth_hk t 
group by phone,PAY_TIME
union all 
--历史信用卡最近一次还款日期距今时长（月份）	
SELECT   phone as contact ,'max_hkmonth' title  
        ,datediff(PAY_TIME,max(endtime)) as values  --最近一次
from tmp_allmonth_hk t 
group by phone,PAY_TIME
union all
--最近还款所属信用卡在还款当月的还款次数	
select phone as contact,'dyhk_cnt' title,count(1)  as values
from
(SELECT    phone                            --手机号 
         ,t.acctno                                      --银行账号
         ,substr(t.endtime,1,7)                    --当月
         ,max(substr(t.endtime,1,7) ) max_time     --最近一次
from tmp_allmonth_hk t  
 group by t.acctno , t.phone,substr(t.endtime,1,7)   -- 月份
) c
group by phone,acctno 
union all
--近12/24/36个月信用卡最早一次还款时间距今时长(月份)

SELECT   contact  ,'12min_hkmonth' title  
       ,datediff(min(endtime),'PAY_TIME') as values  --信用卡最早一次还款时间距今时长
from tmp_12month_hk  t  
--- where t.acctype='C'
group by contact ,pay_time
 
union all 
----近3/6/12/18/24/36个月信用卡还款月份数	
select contact,'12hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                    --手机号 
       -- acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_12month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 
--近12/24/36个月单张信用卡还款的总次数(最多的那张)	
select contact,'12hk_cnt' title,
       max(hk_cnt) as values
from
(select contact,acctno,sum(hk_cnt) hk_cnt from 
 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,count(1) hk_cnt       --还款次数
from tmp_12month_hk  t 
--- where t.acctype='C'
 group by t.acctno,t.contact
) c group by contact,acctno 
) d
group by contact
union all 
--近3/6/12/18/24/36个月合计信用卡还款张数	
select contact , '12card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_12month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/12/18/24/36个月合计信用卡还款次数	
select contact , '12allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact    --手机号 
           ,t.acctno               --银行账号
          ,count(1) hk_cnt       --还款次数
from tmp_12month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/12/18/24/36个月信用卡单次还款的最大金额	
select contact,'12card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_12month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/12/18/24/36个月信用卡单次还款的最小金额	
select contact,'12card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_12month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/12/18/24/36个月合计的还款金额	
SELECT    contact,       --手机号 
                           --银行账号
         '12all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_12month_hk  t 
 group by t.contact
union all 
--近3/6/12/18/24/36个月合计借记卡还款张数	

select contact ,'12dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_12month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/12/18/24/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'12all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_12month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/12/18/24/36个月还款金额最多的信用卡所属银行名称	
select contact,'12all_bankname' title,bankname as values
from 
(select contact,acctno,bankname,max(max_amount) 
 from 
(
SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_12month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
 ) c group by  contact,acctno,bankname
  )d 
union all 
--近3/6/12/18/24/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '12hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_12month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/12/18/24/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/12/18/24/36个月单银行最高还款金额	  重复
select contact,'12hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_12month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact

--2year data

--近24/24/36个月信用卡最早一次还款时间距今时长(月份)

union all
SELECT   contact  ,'24min_hkmonth' title  
       ,datediff(min(endtime),'PAY_TIME') as values  --信用卡最早一次还款时间距今时长
from tmp_24month_hk  t  
--- where t.acctype='C'
group by contact ,pay_time
 
union all 
----近3/6/24/18/24/36个月信用卡还款月份数	
select contact,'24hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                   --手机号 
      --  acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_24month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 
--近24/24/36个月单张信用卡还款的总次数(最多的那张)	
select contact,'24hk_cnt' title,
       max(hk_cnt) as values
from
(select contact,acctno,sum(hk_cnt) hk_cnt from 
 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,count(1) hk_cnt       --还款次数
from tmp_24month_hk  t 
--- where t.acctype='C'
 group by t.acctno,t.contact
) c group by contact,acctno 
) d
group by contact
union all 
--近3/6/24/18/24/36个月合计信用卡还款张数	
select contact , '24card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_24month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/24/18/24/36个月合计信用卡还款次数	
select contact , '24allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact,    --手机号 
           t.acctno               --银行账号
          ,'24card_cnt' title,    
          count(1) hk_cnt       --还款次数
from tmp_24month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/24/18/24/36个月信用卡单次还款的最大金额	
select contact,'24card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_24month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/24/18/24/36个月信用卡单次还款的最小金额	
select contact,'24card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_24month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/24/18/24/36个月合计的还款金额	
SELECT    contact,       --手机号 
         '24all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_24month_hk  t 
 group by t.contact
union all 
--近3/6/24/18/24/36个月合计借记卡还款张数	

select contact ,'24dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_24month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/24/18/24/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'24all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_24month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/24/18/24/36个月还款金额最多的信用卡所属银行名称	
select d.contact,'24all_bankname' title,d.bankname as values
from 
(select c.contact,c.bankname,max(c.max_amount) 
 from 
(
SELECT    t.contact,    --手机号 
          t.acctno              --银行账号
          ,t.bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_24month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,t.bankname
 ) c group by  c.contact,c.acctno,c.bankname
  )d 
union all 
--近3/6/24/18/24/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '24hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_24month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/24/18/24/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/24/18/24/36个月单银行最高还款金额	  重复
select contact,'24hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_24month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact

union all 
--近36/24/36个月信用卡最早一次还款时间距今时长(月份)

SELECT   contact  ,'36min_hkmonth' title  
       ,datediff(min(endtime),'PAY_TIME') as values  --信用卡最早一次还款时间距今时长
from tmp_36month_hk  t  
--- where t.acctype='C'
group by contact ,pay_time
 
union all 
----近3/6/36/18/24/36个月信用卡还款月份数	
select contact,'36hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                    --手机号 
       -- acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_36month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 
--近36/24/36个月单张信用卡还款的总次数(最多的那张)	
select contact,'36hk_cnt' title,
       max(hk_cnt) as values
from
(select contact,acctno,sum(hk_cnt) hk_cnt from 
 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,count(1) hk_cnt       --还款次数
from tmp_36month_hk  t 
--- where t.acctype='C'
 group by t.acctno,t.contact
) c group by contact,acctno 
) d
group by contact
union all 
--近3/6/36/18/24/36个月合计信用卡还款张数	
select contact , '36card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_36month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/36/18/24/36个月合计信用卡还款次数	
select contact , '36allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact,    --手机号 
           t.acctno               --银行账号
          ,'36card_cnt' title,    
          count(1) hk_cnt       --还款次数
from tmp_36month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/36/18/24/36个月信用卡单次还款的最大金额	
select contact,'36card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_36month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/36/18/24/36个月信用卡单次还款的最小金额	
select contact,'36card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_36month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/36/18/24/36个月合计的还款金额	
SELECT    contact,       --手机号 
                           --银行账号
         '36all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_36month_hk  t 
 group by t.acctno,t.contact
union all 
--近3/6/36/18/24/36个月合计借记卡还款张数	

select contact ,'36dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_36month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/36/18/24/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'36all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_36month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/36/18/24/36个月还款金额最多的信用卡所属银行名称	
select contact,'36all_bankname' title,bankname as values
from 
(select contact,acctno,bankname,max(max_amount) 
 from 
(
SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_36month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
 ) c group by  acctno,contact,bankname
  )d 
union all 
--近3/6/36/18/24/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '36hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_36month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/36/18/24/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/36/18/24/36个月单银行最高还款金额	  重复
select contact,'36hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_36month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact

---3month
 
union all 
----近3/6/3/18/3/36个月信用卡还款月份数	
select contact,'3hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                    --手机号 
       -- acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_3month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 

--近3/6/3/18/3/36个月合计信用卡还款张数	
select contact , '3card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_3month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/3/18/3/36个月合计信用卡还款次数	
select contact , '3allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact,    --手机号 
           t.acctno               --银行账号
          ,'3card_cnt' title,    
          count(1) hk_cnt       --还款次数
from tmp_3month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/3/18/3/36个月信用卡单次还款的最大金额	
select contact,'3card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_3month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/3/18/3/36个月信用卡单次还款的最小金额	
select contact,'3card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_3month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/3/18/3/36个月合计的还款金额	
SELECT    contact,       --手机号 
         '3all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_3month_hk  t 
 group by t.contact
union all 
--近3/6/3/18/3/36个月合计借记卡还款张数	

select contact ,'3dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_3month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/3/18/3/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'3all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_3month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/3/18/3/36个月还款金额最多的信用卡所属银行名称	
select contact,'3all_bankname' title,bankname as values
from 
(select contact,acctno,bankname,max(max_amount) 
 from 
(
SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_3month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
 ) c group by  contact,acctno,bankname
  )d 
union all 
--近3/6/3/18/3/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '3hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_3month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/3/18/3/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/3/18/3/36个月单银行最高还款金额	  重复
select contact,'3hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_3month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact









 
union all 
----近3/6/6/18/6/36个月信用卡还款月份数	
select contact,'6hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                    --手机号 
       -- acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_6month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 

--近3/6/6/18/6/36个月合计信用卡还款张数	
select contact , '6card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_6month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/6/18/6/36个月合计信用卡还款次数	
select contact , '6allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact,    --手机号 
           t.acctno               --银行账号
          ,'6card_cnt' title,    
          count(1) hk_cnt       --还款次数
from tmp_6month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/6/18/6/36个月信用卡单次还款的最大金额	
select contact,'6card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_6month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/6/18/6/36个月信用卡单次还款的最小金额	
select contact,'6card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_6month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/6/18/6/36个月合计的还款金额	
SELECT    contact,       --手机号 
                        --银行账号
         '6all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_6month_hk  t 
 group by t.contact
union all 
--近3/6/6/18/6/36个月合计借记卡还款张数	

select contact ,'6dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_6month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/6/18/6/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'6all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_6month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/6/18/6/36个月还款金额最多的信用卡所属银行名称	
select contact,'6all_bankname' title,bankname as values
from 
(select contact,acctno,bankname,max(max_amount) 
 from 
(
SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_6month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
 ) c group by  contact,acctno,bankname
  )d 
union all 
--近3/6/6/18/6/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '6hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_6month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/6/18/6/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/6/18/6/36个月单银行最高还款金额	  重复
select contact,'6hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_6month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact



 
union all 
----近3/6/18/18/18/36个月信用卡还款月份数	
select contact,'18hkmonth' title ,count(month_date) as values
from 
(
SELECT   contact                    --手机号 
       -- acctno                                --账号
        ,substr(t.endtime,1,7)  month_date   --还款月份
from tmp_18month_hk  t 
--- where t.acctype='C'          --信用卡
  group by contact,substr(t.endtime,1,7)  ) c
  group by contact
union all 

--近3/6/18/18/18/36个月合计信用卡还款张数	
select contact , '18card_num' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
from tmp_18month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.contact,t.acctno 
) c
group by contact
union all 
--近3/6/18/18/18/36个月合计信用卡还款次数	
select contact , '18allhk_cnt' title,sum(hk_cnt) as values
from
(SELECT    contact,    --手机号 
           t.acctno               --银行账号
          ,'18card_cnt' title,    
          count(1) hk_cnt       --还款次数
from tmp_18month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact
) c 
group by contact
union all 
--近3/6/18/18/18/36个月信用卡单次还款的最大金额	
select contact,'18card_maxamount' title
       ,max(amount_hk) as values --最大金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_18month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c 
group by contact
union all 
--近3/6/18/18/18/36个月信用卡单次还款的最小金额	
select contact,'18card_minamount' title
       ,min(amount_hk) as values --最小金额
from
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,t.amount  amount_hk   --还款金额
from tmp_18month_hk  t 
--- where t.acctype='C'          --信用卡
 group by t.acctno,t.contact,t.amount
) c
group by contact
union all 
--近3/6/18/18/18/36个月合计的还款金额	
SELECT    contact,       --手机号 
         
         '18all_amount' title
          ,sum(amount) as values  --还款金额
from tmp_18month_hk  t 
 group by t.contact
union all 
--近3/6/18/18/18/36个月合计借记卡还款张数	

select contact ,'18dcatd_cnt' title,count(1) as values  
from
(SELECT   contact,    --手机号 
         t.acctno               --银行账号
   
from tmp_18month_hk  t 
where t.acctype='D'         --借记卡
group by t.contact,t.acctno 
) c 
group by contact
union all 
--近3/6/18/18/18/36个月合计还款的信用卡的银行数量	--还款张数重复
select contact  ,'18all_bankcnt' title,count(1)  as values
from 
(SELECT   contact,    --手机号 
          bankname             --银行名称
from tmp_18month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.contact,bankname 
) c
group by contact

union all 
--近3/6/18/18/18/36个月还款金额最多的信用卡所属银行名称	
select contact,'18all_bankname' title,bankname as values
from 
(select contact,acctno,bankname,max(max_amount) 
 from 
(
SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(amount) max_amount  --还款金额
from tmp_18month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
 ) c group by  contact,acctno,bankname
  )d 
union all 
--近3/6/18/18/18/36个月还款次数最多的信用卡所属银行名称	 
select contact,  '18hkmax_bankname' title,bankname values
from 
(select contact,bankname,max(hk_cnt) as values
from
(select contact,acctno,bankname,sum(hk_cnt) hk_cnt from 
(SELECT    contact,    --手机号 
          t.acctno               --银行账号
          ,bankname            --银行名称
          ,count(1) hk_cnt       --还款次数
from tmp_18month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact,acctno,bankname ) e group by  contact,bankname
) d
union all 
--近3/6/18/18/18/36个月信用卡还款张数/借记卡还款张数	重复
--近3/6/18/18/18/36个月单银行最高还款金额	  重复
select contact,'18hkmax_amount' title,max(max_amount) values 
from
(SELECT    contact,    --手机号 
          t.acctno              --银行账号
          ,bankname           --银行名称
          ,sum(t.amount) max_amount  --还款金额
from tmp_18month_hk  t 
--- where t.acctype='C'             --信用卡
 group by t.acctno,t.contact,bankname
) c 
group by contact
;


drop table  lkl_card_score.alltitle_result;
create table  lkl_card_score.alltitle_result  as
select contact,
round(max(case when title= 'min_hkmonth' then           values else 0 end )/30,0) min_hkmonth ,           
round(max(case when title= 'max_hkmonth' then               values else 0 end )/30,0) max_hkmonth ,               
sum(case when title= 'dyhk_cnt' then                values else 0 end ) dyhk_cnt ,     
round(sum(case when title= '12min_hkmonth' then           values else 0 end )/30,0) c12min_hkmonth ,           
round(max(case when title= '12hkmonth' then               values else 0 end )/30,0) c12hkmonth ,               
sum(case when title= '12hk_cnt' then                values else 0 end ) c12hk_cnt ,                
sum(case when title= '12card_num' then              values else 0 end ) c12card_num ,              
sum(case when title= '12allhk_cnt' then             values else 0 end ) c12allhk_cnt,             
sum(case when title= '12card_cnt' then              values else 0 end ) c12card_cnt ,              
round(sum(case when title= '12card_maxamount' then        values else 0 end )/100,2) c12card_maxamount ,        
round(sum(case when title= '12card_minamount' then        values else 0 end )/100,2) c12card_minamount ,        
round(sum(case when title= '12all_amount' then            values else 0 end )/100,2) c12all_amount,            
sum(case when title= '12dcatd_cnt' then             values else 0 end ) c12dcatd_cnt ,             
sum(case when title= '12all_bankcnt'then             values else 0 end ) c12all_bankcnt ,             
max(case when title= '12all_bankname' then            values else '' end ) c12all_bankname , 
max(case when title= '12hkmax_bankname' then          values else '' end ) c12hkmax_bankname, 

round(sum(case when title= '24min_hkmonth' then           values else '' end )/30,0) c24min_hkmonth ,           
max(case when title= '24hkmonth' then               values else '' end ) c24hkmonth ,               
sum(case when title= '24hk_cnt' then                values else 0 end ) c24hk_cnt ,                
sum(case when title= '24card_cnt' then              values else 0 end ) c24card_cnt ,              
sum(case when title= '24allhk_cnt' then             values else 0 end ) c24allhk_cnt,             
sum(case when title= '24card_num' then              values else 0 end ) c24card_num ,              
round(sum(case when title= '24card_maxamount' then        values else 0 end )/100,2) c24card_maxamount ,        
round(sum(case when title= '24card_minamount' then        values else 0 end )/100,2) c24card_minamount ,        
round(sum(case when title= '24all_amount' then            values else 0 end )/100,2) c24all_amount,            
sum(case when title= '24dcatd_cnt' then             values else 0 end ) c24dcatd_cnt ,             
sum(case when title= '24all_bankcnt'then             values else 0 end ) c24all_bankcnt ,             
max(case when title= '24all_bankname' then            values else '' end ) c24all_bankname , 
max(case when title= '24hkmax_bankname' then          values else '' end ) c24hkmax_bankname ,
                     
round(sum(case when title= '36min_hkmonth' then           values else '' end )/30,0) c36min_hkmonth ,           
max(case when title= '36hkmonth' then               values else '' end ) c36hkmonth ,               
sum(case when title= '36hk_cnt' then                values else 0 end ) c36hk_cnt ,                
sum(case when title= '36card_num' then              values else 0 end ) c36card_num ,              
sum(case when title= '36allhk_cnt' then             values else 0 end ) c36allhk_cnt,             
sum(case when title= '36card_cnt' then              values else 0 end ) c36card_cnt ,              
round(sum(case when title= '36card_maxamount' then        values else 0 end )/100,2) c36card_maxamount ,        
round(sum(case when title= '36card_minamount' then        values else 0 end )/100,2) c36card_minamount ,        
round(sum(case when title= '36all_amount' then            values else 0 end )/100,2) c36all_amount,            
sum(case when title= '36dcatd_cnt' then             values else 0 end ) c36dcatd_cnt ,             
sum(case when title= '36all_bankcnt'then             values else 0 end ) c36all_bankcnt ,             
max(case when title= '36all_bankname' then            values else '' end ) c36all_bankname , 
max(case when title= '36hkmax_bankname' then          values else '' end ) c36hkmax_bankname ,



round(max(case when title= '3hkmonth' then               values else '' end )/30,0) c3hkmonth ,               
sum(case when title= '3card_num' then              values else 0 end ) c3card_num ,              
sum(case when title= '3allhk_cnt' then             values else 0 end ) c3allhk_cnt,             
sum(case when title= '3card_cnt' then              values else 0 end ) c3card_cnt ,              
round(sum(case when title= '3card_maxamount' then        values else 0 end )/100,2) c3card_maxamount ,        
round(sum(case when title= '3card_minamount' then        values else 0 end )/100,2) c3card_minamount ,        
round(sum(case when title= '3all_amount' then            values else 0 end )/100,2) c3all_amount,            
sum(case when title= '3dcatd_cnt' then             values else 0 end ) c3dcatd_cnt ,             
sum(case when title= '3all_bankcnt'then             values else 0 end ) c3all_bankcnt ,             
max(case when title= '3all_bankname' then            values else '' end ) c3all_bankname , 
max(case when title= '3hkmax_bankname' then          values else '' end ) c3hkmax_bankname ,

round(max(case when title= '6hkmonth' then               values else '' end )/30,0) c6hkmonth ,               
sum(case when title= '6card_num' then              values else 0 end ) c6card_num ,              
sum(case when title= '6allhk_cnt' then             values else 0 end ) c6allhk_cnt,             
sum(case when title= '6card_cnt' then              values else 0 end ) c6card_cnt ,              
round(sum(case when title= '6card_maxamount' then        values else 0 end )/100,2) c6card_maxamount ,        
round(sum(case when title= '6card_minamount' then        values else 0 end )/100,2) c6card_minamount ,        
round(sum(case when title= '6all_amount' then            values else 0 end )/100,2) c6all_amount,            
sum(case when title= '6dcatd_cnt' then             values else 0 end ) c6dcatd_cnt ,             
sum(case when title= '6all_bankcnt'then             values else 0 end ) c6all_bankcnt ,             
max(case when title= '6all_bankname' then            values else '' end ) c6all_bankname , 
max(case when title= '6hkmax_bankname' then          values else '' end ) c6hkmax_bankname ,

round(max(case when title= '18hkmonth' then               values else '' end )/30,0) c18hkmonth ,               
sum(case when title= '18card_num' then              values else 0 end ) c18card_num ,              
sum(case when title= '18allhk_cnt' then             values else 0 end ) c18allhk_cnt,             
sum(case when title= '18card_cnt' then              values else 0 end ) c18card_cnt ,              
round(sum(case when title= '18card_maxamount' then        values else 0 end )/100,2) c18card_maxamount ,        
round(sum(case when title= '18card_minamount' then        values else 0 end )/100,2) c18card_minamount ,        
round(sum(case when title= '18all_amount' then            values else 0 end )/100,2) c18all_amount,            
sum(case when title= '18dcatd_cnt' then             values else 0 end ) c18dcatd_cnt ,             
sum(case when title= '18all_bankcnt'then             values else 0 end ) c18all_bankcnt ,             
max(case when title= '18all_bankname' then            values else '' end ) c18all_bankname , 
max(case when title= '18hkmax_bankname' then          values else '' end ) c18hkmax_bankname 

,round(sum(case when title= '3hkmax_amount' then          values else 0 end )/100,2) c3hkmax_amount 
,round(sum(case when title= '6hkmax_amount' then          values else 0 end )/100,2) c6hkmax_amount 
,round(sum(case when title= '12hkmax_amount' then          values else 0 end )/100,2) c12hkmax_amount 
,round(sum(case when title= '18hkmax_amount' then          values else 0 end )/100,2) c18hkmax_amount 
,round(sum(case when title= '24hkmax_amount' then          values else 0 end )/100,2) c24hkmax_amount 
,round(sum(case when title= '36hkmax_amount' then          values else 0 end )/100,2) c36hkmax_amount 
from all_title_table
group by contact;