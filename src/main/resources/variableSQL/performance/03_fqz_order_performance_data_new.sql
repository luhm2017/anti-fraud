use lkl_card_score;

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks=200;
set mapreduce.job.queuename=szrisk;

insert overwrite table fqz_order_performance_data_new partition(year=${f_year},month=${f_month},day=${f_day}) 
select f.order_id,f.performance,f.apply_time,f.type,f.history_due_day,f.current_due_day,u.cert_no,
       (case when fb.orderno is not null then 1   
             when  (fb.orderno is null and f.type = 'pass' and nvl(f.history_due_day,0) <= 0 and nvl(f.current_due_day,0) <= 0 ) then 0  
        else 2 end) as label 
from fqz_order_performance_data f 
join creditloan.s_c_loan_apply  a on f.order_id = a.order_id and f.year=${year} and f.month=${month} and f.day=${day} and a.year = ${year} and a.month = ${month} and a.day = ${day} 
left join fqz_black_contract fb on f.order_id = fb.orderno
join creditloan.s_c_apply_user u on u.id = a.id and u.year = ${year} and u.month = ${month} and u.day = ${day};