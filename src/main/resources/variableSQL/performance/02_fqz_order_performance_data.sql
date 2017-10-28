use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks=200;
set mapreduce.job.queuename=szrisk;

insert overwrite table fqz_order_performance_data partition(year=${year},month=${month},day=${day}) 
select 
ORDER_ID,
case when FAIL_REASON like '%Q%' THEN 'q_refuse' ELSE 'other_refuse' end as performance,
insert_time as apply_time,
'refuse' as type,
0 as  history_due_day,
0 as current_due_day
from creditloan.s_c_loan_apply a
where a.status = 'F'
and a.year = ${year} 
and a.month = ${month} 
and day = ${day}

union all

select
r.order_id,
'null' as performance,
r.apply_time,
'pass' as type,
r.history_due_day,
r.current_due_day
from fqz_contract_performance_data r
where r.year=${year} and r.month=${month} and r.day=${day};