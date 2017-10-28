use lkl_card_score;

--ȫ��������:
--fqz_black_order_data1
--fqz_black_order_data2
--201608--201702��������:
--fqz_order1608to1702_case_data1
--fqz_order1608to1702_case_data2

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;


--һ�ȹ���ȥ��
-- insert overwrite table fqz_order_data1 
-- select a.* from -- fqz_order1608to1702_case_data1 
-- fqz_order_all_data1_distinct a
-- left join fqz_black_order_data1 b on a.c0 = b.c0
-- where b.c0 is null  --ȡȫ���׺�ͬ
-- union all 
-- select * from fqz_black_order_data1 ;
create table  fqz_order_data1 as
select c0,c1,c2,c3,c4 from fqz_order_all_data1
group by c0,c1,c2,c3,c4 ;  --4830930574 --37947468 


--���ȹ���ȥ��
-- insert overwrite table fqz_order_data2  
-- select tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8 from (
-- select * from -- fqz_order1608to1702_case_data2
-- fqz_order_all_data2_distinct 
-- union all
-- select * from fqz_black_order_data2) tab
-- group by tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8;
create table  fqz_order_data2 as
select c0,c1,c2,c3,c4,c5,c6,c7,c8 from  fqz_order_all_data2
group by  c0,c1,c2,c3,c4,c5,c6,c7,c8;


use lkl_card_score;

--ȫ��������:
--fqz_black_order_data1
--fqz_black_order_data2
--201608--201702��������:
--fqz_order1608to1702_case_data1
--fqz_order1608to1702_case_data2

set hive.exec.parallel=8;
set hive.exec.reducers.max=200;
set mapred.reduce.tasks= 200;


--һ�ȹ���ȥ��
-- insert overwrite table fqz_order_data1
-- select a.* from -- fqz_order1608to1702_case_data1
-- fqz_order_all_data1_distinct a
-- left join fqz_black_order_data1 b on a.c0 = b.c0
-- where b.c0 is null  --ȡȫ���׺�ͬ
-- union all
-- select * from fqz_black_order_data1 ;
insert into   fqz_order_data1
select c0,c1,c2,c3,c4 from fqz_order_all_data1_bu
group by c0,c1,c2,c3,c4 ;  --4830930574 --37947468


--���ȹ���ȥ��
-- insert overwrite table fqz_order_data2
-- select tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8 from (
-- select * from -- fqz_order1608to1702_case_data2
-- fqz_order_all_data2_distinct
-- union all
-- select * from fqz_black_order_data2) tab
-- group by tab.c0,tab.c1,tab.c2,tab.c3,tab.c4,tab.c5,tab.c6,tab.c7,tab.c8;
insert into  fqz_order_data2
select c0,c1,c2,c3,c4,c5,c6,c7,c8 from  fqz_order_all_data2_bu
group by  c0,c1,c2,c3,c4,c5,c6,c7,c8;