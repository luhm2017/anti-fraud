use lkl_card_score;
set mapreduce.job.queuename=szbigdata;
drop table phone_variable_tnh_creditcardrepayments_20170724;
create table phone_variable_tnh_creditcardrepayments_20170724 as 
select 
phone        
,nvl(sum_amount6_credit               ,0)            sum_amount1_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount1_credit               
,nvl(max_amount6_credit               ,0)            max_amount1_credit               
,nvl(min_amount6_credit               ,0)            min_amount1_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt1_credit                  
,nvl(sum_amount1_enquirybalance       ,0)            sum_amount1_enquirybalance       
,nvl(sum_amount6_credit               ,0)            sum_amount2_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount2_credit               
,nvl(max_amount6_credit               ,0)            max_amount2_credit               
,nvl(min_amount6_credit               ,0)            min_amount2_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt2_credit                  
,nvl(sum_amount6_credit               ,0)            sum_amount3_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount3_credit               
,nvl(max_amount6_credit               ,0)            max_amount3_credit               
,nvl(min_amount6_credit               ,0)            min_amount3_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt3_credit                  
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount1_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount1_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount1_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount1_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt1_creditcardrepayments    
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount1_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount1_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount1_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount1_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt1_enquiryfee              
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount5_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount5_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount5_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount5_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt5_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount2_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount2_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount2_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount2_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt2_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount4_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount4_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount4_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount4_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt4_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount6_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount6_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount6_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount6_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt6_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount7_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount7_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount7_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount7_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt7_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount9_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount9_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount9_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount9_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt9_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount3_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount3_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount3_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount3_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt3_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount8_creditcardrepayments 
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount8_creditcardrepayments 
,nvl(max_amount15_creditcardrepayments,0)            max_amount8_creditcardrepayments 
,nvl(min_amount15_creditcardrepayments,0)            min_amount8_creditcardrepayments 
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt8_creditcardrepayments    
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount10_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount10_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount10_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount10_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt10_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount11_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount11_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount11_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount11_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt11_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount12_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount12_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount12_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount12_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt12_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount2_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount2_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount2_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount2_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt2_enquiryfee              
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount14_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount14_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount14_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount14_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt14_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount13_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount13_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount13_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount13_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt13_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)            sum_amount15_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)            avg_amount15_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)            max_amount15_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)            min_amount15_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)            sum_cnt15_creditcardrepayments   
,nvl(sum_amount17_creditcardrepayments,0)            sum_amount16_creditcardrepayments
,nvl(avg_amount17_creditcardrepayments,0)            avg_amount16_creditcardrepayments
,nvl(max_amount17_creditcardrepayments,0)            max_amount16_creditcardrepayments
,nvl(min_amount17_creditcardrepayments,0)            min_amount16_creditcardrepayments
,nvl(sum_cnt17_creditcardrepayments   ,0)            sum_cnt16_creditcardrepayments   
,nvl(sum_amount17_creditcardrepayments,0)            sum_amount17_creditcardrepayments
,nvl(avg_amount17_creditcardrepayments,0)            avg_amount17_creditcardrepayments
,nvl(max_amount17_creditcardrepayments,0)            max_amount17_creditcardrepayments
,nvl(min_amount17_creditcardrepayments,0)            min_amount17_creditcardrepayments
,nvl(sum_cnt17_creditcardrepayments   ,0)            sum_cnt17_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount3_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount3_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount3_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount3_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt3_enquiryfee              
,nvl(sum_amount18_creditcardrepayments,0)            sum_amount18_creditcardrepayments
,nvl(avg_amount18_creditcardrepayments,0)            avg_amount18_creditcardrepayments
,nvl(max_amount18_creditcardrepayments,0)            max_amount18_creditcardrepayments
,nvl(min_amount18_creditcardrepayments,0)            min_amount18_creditcardrepayments
,nvl(sum_cnt18_creditcardrepayments   ,0)            sum_cnt18_creditcardrepayments   
,nvl(sum_amount6_credit               ,0)            sum_amount4_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount4_credit               
,nvl(max_amount6_credit               ,0)            max_amount4_credit               
,nvl(min_amount6_credit               ,0)            min_amount4_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt4_credit                  
,nvl(sum_amount19_creditcardrepayments,0)            sum_amount19_creditcardrepayments
,nvl(avg_amount19_creditcardrepayments,0)            avg_amount19_creditcardrepayments
,nvl(max_amount19_creditcardrepayments,0)            max_amount19_creditcardrepayments
,nvl(min_amount19_creditcardrepayments,0)            min_amount19_creditcardrepayments
,nvl(sum_cnt19_creditcardrepayments   ,0)            sum_cnt19_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount4_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount4_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount4_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount4_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt4_enquiryfee              
,nvl(sum_amount20_creditcardrepayments,0)            sum_amount20_creditcardrepayments
,nvl(avg_amount20_creditcardrepayments,0)            avg_amount20_creditcardrepayments
,nvl(max_amount20_creditcardrepayments,0)            max_amount20_creditcardrepayments
,nvl(min_amount20_creditcardrepayments,0)            min_amount20_creditcardrepayments
,nvl(sum_cnt20_creditcardrepayments   ,0)            sum_cnt20_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount5_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount5_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount5_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount5_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt5_enquiryfee              
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount6_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount6_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount6_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount6_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt6_enquiryfee              
,nvl(sum_amount21_creditcardrepayments,0)            sum_amount21_creditcardrepayments
,nvl(avg_amount21_creditcardrepayments,0)            avg_amount21_creditcardrepayments
,nvl(max_amount21_creditcardrepayments,0)            max_amount21_creditcardrepayments
,nvl(min_amount21_creditcardrepayments,0)            min_amount21_creditcardrepayments
,nvl(sum_cnt21_creditcardrepayments   ,0)            sum_cnt21_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount7_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount7_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount7_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt7_enquiryfee              
,nvl(min_amount18_enquiryfee          ,0)            min_amount7_enquiryfee           
,nvl(sum_amount22_creditcardrepayments,0)            sum_amount22_creditcardrepayments
,nvl(avg_amount22_creditcardrepayments,0)            avg_amount22_creditcardrepayments
,nvl(max_amount22_creditcardrepayments,0)            max_amount22_creditcardrepayments
,nvl(min_amount22_creditcardrepayments,0)            min_amount22_creditcardrepayments
,nvl(sum_cnt22_creditcardrepayments   ,0)            sum_cnt22_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount8_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount8_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount8_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt8_enquiryfee              
,nvl(min_amount18_enquiryfee          ,0)            min_amount8_enquiryfee           
,nvl(sum_amount23_creditcardrepayments,0)            sum_amount23_creditcardrepayments
,nvl(avg_amount23_creditcardrepayments,0)            avg_amount23_creditcardrepayments
,nvl(max_amount23_creditcardrepayments,0)            max_amount23_creditcardrepayments
,nvl(min_amount23_creditcardrepayments,0)            min_amount23_creditcardrepayments
,nvl(sum_cnt23_creditcardrepayments   ,0)            sum_cnt23_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount9_enquiryfee           
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount9_enquiryfee           
,nvl(max_amount18_enquiryfee          ,0)            max_amount9_enquiryfee           
,nvl(min_amount18_enquiryfee          ,0)            min_amount9_enquiryfee           
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt9_enquiryfee              
,nvl(sum_amount24_creditcardrepayments,0)            sum_amount24_creditcardrepayments
,nvl(avg_amount24_creditcardrepayments,0)            avg_amount24_creditcardrepayments
,nvl(max_amount24_creditcardrepayments,0)            max_amount24_creditcardrepayments
,nvl(min_amount24_creditcardrepayments,0)            min_amount24_creditcardrepayments
,nvl(sum_cnt24_creditcardrepayments   ,0)            sum_cnt24_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount10_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount10_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount10_enquiryfee          
,nvl(min_amount18_enquiryfee          ,0)            min_amount10_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt10_enquiryfee             
,nvl(sum_amount25_creditcardrepayments,0)            sum_amount25_creditcardrepayments
,nvl(avg_amount25_creditcardrepayments,0)            avg_amount25_creditcardrepayments
,nvl(max_amount25_creditcardrepayments,0)            max_amount25_creditcardrepayments
,nvl(min_amount25_creditcardrepayments,0)            min_amount25_creditcardrepayments
,nvl(sum_cnt25_creditcardrepayments   ,0)            sum_cnt25_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount11_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount11_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount11_enquiryfee          
,nvl(min_amount18_enquiryfee          ,0)            min_amount11_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt11_enquiryfee             
,nvl(sum_amount26_creditcardrepayments,0)            sum_amount26_creditcardrepayments
,nvl(avg_amount26_creditcardrepayments,0)            avg_amount26_creditcardrepayments
,nvl(max_amount26_creditcardrepayments,0)            max_amount26_creditcardrepayments
,nvl(min_amount26_creditcardrepayments,0)            min_amount26_creditcardrepayments
,nvl(sum_cnt26_creditcardrepayments   ,0)            sum_cnt26_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount12_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount12_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount12_enquiryfee          
,nvl(min_amount18_enquiryfee          ,0)            min_amount12_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt12_enquiryfee             
,nvl(sum_amount27_creditcardrepayments,0)            sum_amount27_creditcardrepayments
,nvl(avg_amount27_creditcardrepayments,0)            avg_amount27_creditcardrepayments
,nvl(max_amount27_creditcardrepayments,0)            max_amount27_creditcardrepayments
,nvl(min_amount27_creditcardrepayments,0)            min_amount27_creditcardrepayments
,nvl(sum_cnt27_creditcardrepayments   ,0)            sum_cnt27_creditcardrepayments   
,nvl(avg_amount18_enquiryfee          ,0)            sum_amount13_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            avg_amount13_enquiryfee          
,nvl(sum_amount18_enquiryfee          ,0)            max_amount13_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            min_amount13_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt13_enquiryfee             
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount14_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount14_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount14_enquiryfee          
,nvl(min_amount18_enquiryfee          ,0)            min_amount14_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt14_enquiryfee             
,nvl(sum_amount28_creditcardrepayments,0)            sum_amount28_creditcardrepayments
,nvl(avg_amount28_creditcardrepayments,0)            avg_amount28_creditcardrepayments
,nvl(max_amount28_creditcardrepayments,0)            max_amount28_creditcardrepayments
,nvl(min_amount28_creditcardrepayments,0)            min_amount28_creditcardrepayments
,nvl(sum_cnt28_creditcardrepayments   ,0)            sum_cnt28_creditcardrepayments   
,nvl(sum_amount29_creditcardrepayments,0)            sum_amount29_creditcardrepayments
,nvl(avg_amount29_creditcardrepayments,0)            avg_amount29_creditcardrepayments
,nvl(max_amount29_creditcardrepayments,0)            max_amount29_creditcardrepayments
,nvl(min_amount29_creditcardrepayments,0)            min_amount29_creditcardrepayments
,nvl(sum_cnt29_creditcardrepayments   ,0)            sum_cnt29_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount15_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount15_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount15_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt15_enquiryfee             
,nvl(min_amount18_enquiryfee          ,0)            min_amount15_enquiryfee          
,nvl(sum_amount19_enquiryfee          ,0)            sum_amount16_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)            avg_amount16_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)            max_amount16_enquiryfee          
,nvl(min_amount19_enquiryfee          ,0)            min_amount16_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)            sum_cnt16_enquiryfee             
,nvl(sum_amount30_creditcardrepayments,0)            sum_amount30_creditcardrepayments
,nvl(avg_amount30_creditcardrepayments,0)            avg_amount30_creditcardrepayments
,nvl(max_amount30_creditcardrepayments,0)            max_amount30_creditcardrepayments
,nvl(min_amount30_creditcardrepayments,0)            min_amount30_creditcardrepayments
,nvl(sum_cnt30_creditcardrepayments   ,0)            sum_cnt30_creditcardrepayments   
,nvl(sum_amount19_enquiryfee          ,0)            sum_amount17_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)            avg_amount17_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)            max_amount17_enquiryfee          
,nvl(min_amount19_enquiryfee          ,0)            min_amount17_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)            sum_cnt17_enquiryfee             
,nvl(sum_amount6_credit               ,0)            sum_amount5_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount5_credit               
,nvl(max_amount6_credit               ,0)            max_amount5_credit               
,nvl(min_amount6_credit               ,0)            min_amount5_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt5_credit                  
,nvl(sum_amount31_creditcardrepayments,0)            sum_amount31_creditcardrepayments
,nvl(avg_amount31_creditcardrepayments,0)            avg_amount31_creditcardrepayments
,nvl(max_amount31_creditcardrepayments,0)            max_amount31_creditcardrepayments
,nvl(min_amount31_creditcardrepayments,0)            min_amount31_creditcardrepayments
,nvl(sum_cnt31_creditcardrepayments   ,0)            sum_cnt31_creditcardrepayments   
,nvl(sum_amount18_enquiryfee          ,0)            sum_amount18_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)            avg_amount18_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)            max_amount18_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)            sum_cnt18_enquiryfee             
,nvl(min_amount18_enquiryfee          ,0)            min_amount18_enquiryfee          
,nvl(sum_amount19_enquiryfee          ,0)            sum_amount19_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)            avg_amount19_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)            max_amount19_enquiryfee          
,nvl(min_amount19_enquiryfee          ,0)            min_amount19_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)            sum_cnt19_enquiryfee             
,nvl(sum_amount32_creditcardrepayments,0)            sum_amount32_creditcardrepayments
,nvl(avg_amount32_creditcardrepayments,0)            avg_amount32_creditcardrepayments
,nvl(max_amount32_creditcardrepayments,0)            max_amount32_creditcardrepayments
,nvl(min_amount32_creditcardrepayments,0)            min_amount32_creditcardrepayments
,nvl(sum_cnt32_creditcardrepayments   ,0)            sum_cnt32_creditcardrepayments   
,nvl(sum_amount20_enquiryfee          ,0)            sum_amount20_enquiryfee          
,nvl(avg_amount20_enquiryfee          ,0)            avg_amount20_enquiryfee          
,nvl(max_amount20_enquiryfee          ,0)            max_amount20_enquiryfee          
,nvl(min_amount20_enquiryfee          ,0)            min_amount20_enquiryfee          
,nvl(sum_cnt20_enquiryfee             ,0)            sum_cnt20_enquiryfee             
,nvl(sum_amount33_creditcardrepayments,0)            sum_amount33_creditcardrepayments
,nvl(avg_amount33_creditcardrepayments,0)            avg_amount33_creditcardrepayments
,nvl(max_amount33_creditcardrepayments,0)            max_amount33_creditcardrepayments
,nvl(min_amount33_creditcardrepayments,0)            min_amount33_creditcardrepayments
,nvl(sum_cnt33_creditcardrepayments   ,0)            sum_cnt33_creditcardrepayments   
,nvl(sum_amount21_enquiryfee          ,0)            sum_amount21_enquiryfee          
,nvl(avg_amount21_enquiryfee          ,0)            avg_amount21_enquiryfee          
,nvl(max_amount21_enquiryfee          ,0)            max_amount21_enquiryfee          
,nvl(sum_cnt21_enquiryfee             ,0)            sum_cnt21_enquiryfee             
,nvl(min_amount21_enquiryfee          ,0)            min_amount21_enquiryfee          
,nvl(sum_amount34_creditcardrepayments,0)            sum_amount34_creditcardrepayments
,nvl(avg_amount34_creditcardrepayments,0)            avg_amount34_creditcardrepayments
,nvl(max_amount34_creditcardrepayments,0)            max_amount34_creditcardrepayments
,nvl(min_amount34_creditcardrepayments,0)            min_amount34_creditcardrepayments
,nvl(sum_cnt34_creditcardrepayments   ,0)            sum_cnt34_creditcardrepayments   
,nvl(sum_amount22_enquiryfee          ,0)            sum_amount22_enquiryfee          
,nvl(avg_amount22_enquiryfee          ,0)            avg_amount22_enquiryfee          
,nvl(max_amount22_enquiryfee          ,0)            max_amount22_enquiryfee          
,nvl(min_amount22_enquiryfee          ,0)            min_amount22_enquiryfee          
,nvl(sum_cnt22_enquiryfee             ,0)            sum_cnt22_enquiryfee             
,nvl(sum_amount35_creditcardrepayments,0)            sum_amount35_creditcardrepayments
,nvl(avg_amount35_creditcardrepayments,0)            avg_amount35_creditcardrepayments
,nvl(max_amount35_creditcardrepayments,0)            max_amount35_creditcardrepayments
,nvl(min_amount35_creditcardrepayments,0)            min_amount35_creditcardrepayments
,nvl(sum_cnt35_creditcardrepayments   ,0)            sum_cnt35_creditcardrepayments   
,nvl(sum_amount23_enquiryfee          ,0)            sum_amount23_enquiryfee          
,nvl(avg_amount23_enquiryfee          ,0)            avg_amount23_enquiryfee          
,nvl(max_amount23_enquiryfee          ,0)            max_amount23_enquiryfee          
,nvl(min_amount23_enquiryfee          ,0)            min_amount23_enquiryfee          
,nvl(sum_cnt23_enquiryfee             ,0)            sum_cnt23_enquiryfee             
,nvl(sum_amount6_credit               ,0)            sum_amount6_credit               
,nvl(avg_amount6_credit               ,0)            avg_amount6_credit               
,nvl(max_amount6_credit               ,0)            max_amount6_credit               
,nvl(min_amount6_credit               ,0)            min_amount6_credit               
,nvl(sum_cnt6_credit                  ,0)            sum_cnt6_credit                  
,nvl(sum_amount36_creditcardrepayments,0)            sum_amount36_creditcardrepayments
,nvl(avg_amount36_creditcardrepayments,0)            avg_amount36_creditcardrepayments
,nvl(max_amount36_creditcardrepayments,0)            max_amount36_creditcardrepayments
,nvl(min_amount36_creditcardrepayments,0)            min_amount36_creditcardrepayments
,nvl(sum_cnt36_creditcardrepayments   ,0)            sum_cnt36_creditcardrepayments   
,nvl(sum_amount24_enquiryfee          ,0)            sum_amount24_enquiryfee          
,nvl(avg_amount24_enquiryfee          ,0)            avg_amount24_enquiryfee          
,nvl(max_amount24_enquiryfee          ,0)            max_amount24_enquiryfee          
,nvl(min_amount24_enquiryfee          ,0)            min_amount24_enquiryfee          
,nvl(sum_cnt24_enquiryfee             ,0)            sum_cnt24_enquiryfee             
,nvl(sum_amount25_enquiryfee          ,0)            sum_amount25_enquiryfee          
,nvl(avg_amount25_enquiryfee          ,0)            avg_amount25_enquiryfee          
,nvl(max_amount25_enquiryfee          ,0)            max_amount25_enquiryfee          
,nvl(sum_cnt25_enquiryfee             ,0)            sum_cnt25_enquiryfee             
,nvl(min_amount25_enquiryfee          ,0)            min_amount25_enquiryfee          
,nvl(sum_amount37_creditcardrepayments,0)            sum_amount37_creditcardrepayments
,nvl(avg_amount37_creditcardrepayments,0)            avg_amount37_creditcardrepayments
,nvl(max_amount37_creditcardrepayments,0)            max_amount37_creditcardrepayments
,nvl(min_amount37_creditcardrepayments,0)            min_amount37_creditcardrepayments
,nvl(sum_cnt37_creditcardrepayments   ,0)            sum_cnt37_creditcardrepayments   
,nvl(sum_amount26_enquiryfee          ,0)            sum_amount26_enquiryfee          
,nvl(avg_amount26_enquiryfee          ,0)            avg_amount26_enquiryfee          
,nvl(max_amount26_enquiryfee          ,0)            max_amount26_enquiryfee          
,nvl(min_amount26_enquiryfee          ,0)            min_amount26_enquiryfee          
,nvl(sum_cnt26_enquiryfee             ,0)            sum_cnt26_enquiryfee             
,nvl(sum_amount38_creditcardrepayments,0)            sum_amount38_creditcardrepayments
,nvl(avg_amount38_creditcardrepayments,0)            avg_amount38_creditcardrepayments
,nvl(max_amount38_creditcardrepayments,0)            max_amount38_creditcardrepayments
,nvl(min_amount38_creditcardrepayments,0)            min_amount38_creditcardrepayments
,nvl(sum_cnt38_creditcardrepayments   ,0)            sum_cnt38_creditcardrepayments   
,nvl(sum_amount7_credit               ,0)            sum_amount7_credit               
,nvl(avg_amount7_credit               ,0)            avg_amount7_credit               
,nvl(max_amount7_credit               ,0)            max_amount7_credit               
,nvl(min_amount7_credit               ,0)            min_amount7_credit               
,nvl(sum_cnt7_credit                  ,0)            sum_cnt7_credit                  
,nvl(sum_amount27_enquiryfee          ,0)            sum_amount27_enquiryfee          
,nvl(avg_amount27_enquiryfee          ,0)            avg_amount27_enquiryfee          
,nvl(max_amount27_enquiryfee          ,0)            max_amount27_enquiryfee          
,nvl(min_amount27_enquiryfee          ,0)            min_amount27_enquiryfee          
,nvl(sum_cnt27_enquiryfee             ,0)            sum_cnt27_enquiryfee             
,nvl(sum_amount39_creditcardrepayments,0)            sum_amount39_creditcardrepayments
,nvl(avg_amount39_creditcardrepayments,0)            avg_amount39_creditcardrepayments
,nvl(max_amount39_creditcardrepayments,0)            max_amount39_creditcardrepayments
,nvl(min_amount39_creditcardrepayments,0)            min_amount39_creditcardrepayments
,nvl(sum_cnt39_creditcardrepayments   ,0)            sum_cnt39_creditcardrepayments   
,nvl(sum_amount28_enquiryfee          ,0)            sum_amount28_enquiryfee          
,nvl(avg_amount28_enquiryfee          ,0)            avg_amount28_enquiryfee          
,nvl(max_amount28_enquiryfee          ,0)            max_amount28_enquiryfee          
,nvl(min_amount28_enquiryfee          ,0)            min_amount28_enquiryfee          
,nvl(sum_cnt28_enquiryfee             ,0)            sum_cnt28_enquiryfee             
,nvl(sum_amount40_creditcardrepayments,0)            sum_amount40_creditcardrepayments
,nvl(avg_amount40_creditcardrepayments,0)            avg_amount40_creditcardrepayments
,nvl(max_amount40_creditcardrepayments,0)            max_amount40_creditcardrepayments
,nvl(min_amount40_creditcardrepayments,0)            min_amount40_creditcardrepayments
,nvl(sum_cnt40_creditcardrepayments   ,0)            sum_cnt40_creditcardrepayments   
,nvl(sum_amount29_enquiryfee          ,0)            sum_amount29_enquiryfee          
,nvl(avg_amount29_enquiryfee          ,0)            avg_amount29_enquiryfee          
,nvl(max_amount29_enquiryfee          ,0)            max_amount29_enquiryfee          
,nvl(min_amount29_enquiryfee          ,0)            min_amount29_enquiryfee          
,nvl(sum_cnt29_enquiryfee             ,0)            sum_cnt29_enquiryfee             
,nvl(sum_amount8_credit               ,0)            sum_amount8_credit               
,nvl(avg_amount8_credit               ,0)            avg_amount8_credit               
,nvl(max_amount8_credit               ,0)            max_amount8_credit               
,nvl(min_amount8_credit               ,0)            min_amount8_credit               
,nvl(sum_cnt8_credit                  ,0)            sum_cnt8_credit                  
,nvl(sum_amount41_creditcardrepayments,0)            sum_amount41_creditcardrepayments
,nvl(avg_amount41_creditcardrepayments,0)            avg_amount41_creditcardrepayments
,nvl(max_amount41_creditcardrepayments,0)            max_amount41_creditcardrepayments
,nvl(min_amount41_creditcardrepayments,0)            min_amount41_creditcardrepayments
,nvl(sum_cnt41_creditcardrepayments   ,0)            sum_cnt41_creditcardrepayments   
,nvl(sum_amount30_enquiryfee          ,0)            sum_amount30_enquiryfee          
,nvl(avg_amount30_enquiryfee          ,0)            avg_amount30_enquiryfee          
,nvl(max_amount30_enquiryfee          ,0)            max_amount30_enquiryfee          
,nvl(min_amount30_enquiryfee          ,0)            min_amount30_enquiryfee          
,nvl(sum_cnt30_enquiryfee             ,0)            sum_cnt30_enquiryfee             
,nvl(sum_amount42_creditcardrepayments,0)            sum_amount42_creditcardrepayments
,nvl(avg_amount42_creditcardrepayments,0)            avg_amount42_creditcardrepayments
,nvl(max_amount42_creditcardrepayments,0)            max_amount42_creditcardrepayments
,nvl(min_amount42_creditcardrepayments,0)            min_amount42_creditcardrepayments
,nvl(sum_cnt42_creditcardrepayments   ,0)            sum_cnt42_creditcardrepayments   
,nvl(sum_amount31_enquiryfee          ,0)            sum_amount31_enquiryfee          
,nvl(avg_amount31_enquiryfee          ,0)            avg_amount31_enquiryfee          
,nvl(max_amount31_enquiryfee          ,0)            max_amount31_enquiryfee          
,nvl(min_amount31_enquiryfee          ,0)            min_amount31_enquiryfee          
,nvl(sum_cnt31_enquiryfee             ,0)            sum_cnt31_enquiryfee             
,nvl(sum_amount9_credit               ,0)            sum_amount9_credit               
,nvl(avg_amount9_credit               ,0)            avg_amount9_credit               
,nvl(max_amount9_credit               ,0)            max_amount9_credit               
,nvl(min_amount9_credit               ,0)            min_amount9_credit               
,nvl(sum_cnt9_credit                  ,0)            sum_cnt9_credit                  
,nvl(sum_amount43_creditcardrepayments,0)            sum_amount43_creditcardrepayments
,nvl(avg_amount43_creditcardrepayments,0)            avg_amount43_creditcardrepayments
,nvl(max_amount43_creditcardrepayments,0)            max_amount43_creditcardrepayments
,nvl(min_amount43_creditcardrepayments,0)            min_amount43_creditcardrepayments
,nvl(sum_cnt43_creditcardrepayments   ,0)            sum_cnt43_creditcardrepayments   
,nvl(sum_amount32_enquiryfee          ,0)            sum_amount32_enquiryfee          
,nvl(avg_amount32_enquiryfee          ,0)            avg_amount32_enquiryfee          
,nvl(max_amount32_enquiryfee          ,0)            max_amount32_enquiryfee          
,nvl(sum_cnt32_enquiryfee             ,0)            sum_cnt32_enquiryfee             
,nvl(min_amount32_enquiryfee          ,0)            min_amount32_enquiryfee          
,nvl(sum_amount44_creditcardrepayments,0)            sum_amount44_creditcardrepayments
,nvl(avg_amount44_creditcardrepayments,0)            avg_amount44_creditcardrepayments
,nvl(max_amount44_creditcardrepayments,0)            max_amount44_creditcardrepayments
,nvl(min_amount44_creditcardrepayments,0)            min_amount44_creditcardrepayments
,nvl(sum_cnt44_creditcardrepayments   ,0)            sum_cnt44_creditcardrepayments   
,nvl(sum_amount33_enquiryfee          ,0)            sum_amount33_enquiryfee          
,nvl(avg_amount33_enquiryfee          ,0)            avg_amount33_enquiryfee          
,nvl(max_amount33_enquiryfee          ,0)            max_amount33_enquiryfee          
,nvl(sum_cnt33_enquiryfee             ,0)            sum_cnt33_enquiryfee             
,nvl(min_amount32_enquiryfee          ,0)            min_amount33_enquiryfee          


     
from phone_variable_yfq_tnh_2;
;    
     
     
drop table phone_variable_yfq_creditcardrepayments_20170724;
create table phone_variable_yfq_creditcardrepayments_20170724 as 
select 
phone   
,nvl(sum_amount20_creditcardrepayments,0)                  sum_amount20_creditcardrepayments       
,nvl(avg_amount20_creditcardrepayments,0)                  avg_amount20_creditcardrepayments
,nvl(max_amount20_creditcardrepayments,0)                  max_amount20_creditcardrepayments
,nvl(min_amount20_creditcardrepayments,0)                  min_amount20_creditcardrepayments
,nvl(sum_cnt20_creditcardrepayments   ,0)                  sum_cnt20_creditcardrepayments   
,nvl(sum_amount19_creditcardrepayments,0)                  sum_amount19_creditcardrepayments
,nvl(avg_amount19_creditcardrepayments,0)                  avg_amount19_creditcardrepayments
,nvl(max_amount19_creditcardrepayments,0)                  max_amount19_creditcardrepayments
,nvl(min_amount19_creditcardrepayments,0)                  min_amount19_creditcardrepayments
,nvl(sum_cnt19_creditcardrepayments   ,0)                  sum_cnt19_creditcardrepayments   
,nvl(sum_amount21_creditcardrepayments,0)                  sum_amount21_creditcardrepayments
,nvl(avg_amount21_creditcardrepayments,0)                  avg_amount21_creditcardrepayments
,nvl(max_amount21_creditcardrepayments,0)                  max_amount21_creditcardrepayments
,nvl(min_amount21_creditcardrepayments,0)                  min_amount21_creditcardrepayments
,nvl(sum_cnt21_creditcardrepayments   ,0)                  sum_cnt21_creditcardrepayments   
,nvl(sum_amount18_creditcardrepayments,0)                  sum_amount18_creditcardrepayments
,nvl(avg_amount18_creditcardrepayments,0)                  avg_amount18_creditcardrepayments
,nvl(max_amount18_creditcardrepayments,0)                  max_amount18_creditcardrepayments
,nvl(min_amount18_creditcardrepayments,0)                  min_amount18_creditcardrepayments
,nvl(sum_cnt18_creditcardrepayments   ,0)                  sum_cnt18_creditcardrepayments   
,nvl(sum_amount22_creditcardrepayments,0)                  sum_amount22_creditcardrepayments
,nvl(avg_amount22_creditcardrepayments,0)                  avg_amount22_creditcardrepayments
,nvl(max_amount22_creditcardrepayments,0)                  max_amount22_creditcardrepayments
,nvl(min_amount22_creditcardrepayments,0)                  min_amount22_creditcardrepayments
,nvl(sum_cnt22_creditcardrepayments   ,0)                  sum_cnt22_creditcardrepayments   
,nvl(sum_amount17_creditcardrepayments,0)                  sum_amount17_creditcardrepayments
,nvl(avg_amount17_creditcardrepayments,0)                  avg_amount17_creditcardrepayments
,nvl(max_amount17_creditcardrepayments,0)                  max_amount17_creditcardrepayments
,nvl(min_amount17_creditcardrepayments,0)                  min_amount17_creditcardrepayments
,nvl(sum_cnt17_creditcardrepayments   ,0)                  sum_cnt17_creditcardrepayments   
,nvl(sum_amount23_creditcardrepayments,0)                  sum_amount23_creditcardrepayments
,nvl(avg_amount23_creditcardrepayments,0)                  avg_amount23_creditcardrepayments
,nvl(max_amount23_creditcardrepayments,0)                  max_amount23_creditcardrepayments
,nvl(min_amount23_creditcardrepayments,0)                  min_amount23_creditcardrepayments
,nvl(sum_cnt23_creditcardrepayments   ,0)                  sum_cnt23_creditcardrepayments   
,nvl(sum_amount16_creditcardrepayments,0)                  sum_amount16_creditcardrepayments
,nvl(avg_amount16_creditcardrepayments,0)                  avg_amount16_creditcardrepayments
,nvl(max_amount16_creditcardrepayments,0)                  max_amount16_creditcardrepayments
,nvl(min_amount16_creditcardrepayments,0)                  min_amount16_creditcardrepayments
,nvl(sum_cnt16_creditcardrepayments   ,0)                  sum_cnt16_creditcardrepayments   
,nvl(sum_amount24_creditcardrepayments,0)                  sum_amount24_creditcardrepayments
,nvl(avg_amount24_creditcardrepayments,0)                  avg_amount24_creditcardrepayments
,nvl(max_amount24_creditcardrepayments,0)                  max_amount24_creditcardrepayments
,nvl(min_amount24_creditcardrepayments,0)                  min_amount24_creditcardrepayments
,nvl(sum_cnt24_creditcardrepayments   ,0)                  sum_cnt24_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)                  sum_amount15_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)                  avg_amount15_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)                  max_amount15_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)                  min_amount15_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)                  sum_cnt15_creditcardrepayments   
,nvl(sum_amount25_creditcardrepayments,0)                  sum_amount25_creditcardrepayments
,nvl(avg_amount25_creditcardrepayments,0)                  avg_amount25_creditcardrepayments
,nvl(max_amount25_creditcardrepayments,0)                  max_amount25_creditcardrepayments
,nvl(min_amount25_creditcardrepayments,0)                  min_amount25_creditcardrepayments
,nvl(sum_cnt25_creditcardrepayments   ,0)                  sum_cnt25_creditcardrepayments   
,nvl(sum_amount14_creditcardrepayments,0)                  sum_amount14_creditcardrepayments
,nvl(avg_amount14_creditcardrepayments,0)                  avg_amount14_creditcardrepayments
,nvl(max_amount14_creditcardrepayments,0)                  max_amount14_creditcardrepayments
,nvl(min_amount14_creditcardrepayments,0)                  min_amount14_creditcardrepayments
,nvl(sum_cnt14_creditcardrepayments   ,0)                  sum_cnt14_creditcardrepayments   
,nvl(sum_amount26_creditcardrepayments,0)                  sum_amount26_creditcardrepayments
,nvl(avg_amount26_creditcardrepayments,0)                  avg_amount26_creditcardrepayments
,nvl(max_amount26_creditcardrepayments,0)                  max_amount26_creditcardrepayments
,nvl(min_amount26_creditcardrepayments,0)                  min_amount26_creditcardrepayments
,nvl(sum_cnt26_creditcardrepayments   ,0)                  sum_cnt26_creditcardrepayments   
,nvl(sum_amount13_creditcardrepayments,0)                  sum_amount13_creditcardrepayments
,nvl(avg_amount13_creditcardrepayments,0)                  avg_amount13_creditcardrepayments
,nvl(max_amount13_creditcardrepayments,0)                  max_amount13_creditcardrepayments
,nvl(min_amount13_creditcardrepayments,0)                  min_amount13_creditcardrepayments
,nvl(sum_cnt13_creditcardrepayments   ,0)                  sum_cnt13_creditcardrepayments   
,nvl(sum_amount27_creditcardrepayments,0)                  sum_amount27_creditcardrepayments
,nvl(avg_amount27_creditcardrepayments,0)                  avg_amount27_creditcardrepayments
,nvl(max_amount27_creditcardrepayments,0)                  max_amount27_creditcardrepayments
,nvl(min_amount27_creditcardrepayments,0)                  min_amount27_creditcardrepayments
,nvl(sum_cnt27_creditcardrepayments   ,0)                  sum_cnt27_creditcardrepayments   
,nvl(sum_amount12_creditcardrepayments,0)                  sum_amount12_creditcardrepayments
,nvl(avg_amount12_creditcardrepayments,0)                  avg_amount12_creditcardrepayments
,nvl(max_amount12_creditcardrepayments,0)                  max_amount12_creditcardrepayments
,nvl(min_amount12_creditcardrepayments,0)                  min_amount12_creditcardrepayments
,nvl(sum_cnt12_creditcardrepayments   ,0)                  sum_cnt12_creditcardrepayments   
,nvl(sum_amount28_creditcardrepayments,0)                  sum_amount28_creditcardrepayments
,nvl(avg_amount28_creditcardrepayments,0)                  avg_amount28_creditcardrepayments
,nvl(max_amount28_creditcardrepayments,0)                  max_amount28_creditcardrepayments
,nvl(min_amount28_creditcardrepayments,0)                  min_amount28_creditcardrepayments
,nvl(sum_cnt28_creditcardrepayments   ,0)                  sum_cnt28_creditcardrepayments   
,nvl(sum_amount11_creditcardrepayments,0)                  sum_amount11_creditcardrepayments
,nvl(avg_amount11_creditcardrepayments,0)                  avg_amount11_creditcardrepayments
,nvl(max_amount11_creditcardrepayments,0)                  max_amount11_creditcardrepayments
,nvl(min_amount11_creditcardrepayments,0)                  min_amount11_creditcardrepayments
,nvl(sum_cnt11_creditcardrepayments   ,0)                  sum_cnt11_creditcardrepayments   
,nvl(sum_amount29_creditcardrepayments,0)                  sum_amount29_creditcardrepayments
,nvl(avg_amount29_creditcardrepayments,0)                  avg_amount29_creditcardrepayments
,nvl(max_amount29_creditcardrepayments,0)                  max_amount29_creditcardrepayments
,nvl(min_amount29_creditcardrepayments,0)                  min_amount29_creditcardrepayments
,nvl(sum_cnt29_creditcardrepayments   ,0)                  sum_cnt29_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount10_creditcardrepayments
,nvl(avg_amount10_creditcardrepayments,0)                  avg_amount10_creditcardrepayments
,nvl(max_amount10_creditcardrepayments,0)                  max_amount10_creditcardrepayments
,nvl(min_amount10_creditcardrepayments,0)                  min_amount10_creditcardrepayments
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt10_creditcardrepayments   
,nvl(sum_amount30_creditcardrepayments,0)                  sum_amount30_creditcardrepayments
,nvl(avg_amount30_creditcardrepayments,0)                  avg_amount30_creditcardrepayments
,nvl(max_amount30_creditcardrepayments,0)                  max_amount30_creditcardrepayments
,nvl(min_amount30_creditcardrepayments,0)                  min_amount30_creditcardrepayments
,nvl(sum_cnt30_creditcardrepayments   ,0)                  sum_cnt30_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount9_creditcardrepayments 
,nvl(avg_amount10_creditcardrepayments,0)                  avg_amount9_creditcardrepayments 
,nvl(max_amount10_creditcardrepayments,0)                  max_amount9_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount9_creditcardrepayments 
,nvl(sum_cnt9_creditcardrepayments    ,0)                  sum_cnt9_creditcardrepayments    
,nvl(sum_amount31_creditcardrepayments,0)                  sum_amount31_creditcardrepayments
,nvl(avg_amount31_creditcardrepayments,0)                  avg_amount31_creditcardrepayments
,nvl(max_amount31_creditcardrepayments,0)                  max_amount31_creditcardrepayments
,nvl(min_amount31_creditcardrepayments,0)                  min_amount31_creditcardrepayments
,nvl(sum_cnt31_creditcardrepayments   ,0)                  sum_cnt31_creditcardrepayments   
,nvl(sum_amount8_creditcardrepayments ,0)                  sum_amount8_creditcardrepayments 
,nvl(avg_amount10_creditcardrepayments,0)                  avg_amount8_creditcardrepayments 
,nvl(max_amount10_creditcardrepayments,0)                  max_amount8_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount8_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt8_creditcardrepayments    
,nvl(sum_amount32_creditcardrepayments,0)                  sum_amount32_creditcardrepayments
,nvl(avg_amount32_creditcardrepayments,0)                  avg_amount32_creditcardrepayments
,nvl(max_amount32_creditcardrepayments,0)                  max_amount32_creditcardrepayments
,nvl(min_amount32_creditcardrepayments,0)                  min_amount32_creditcardrepayments
,nvl(sum_cnt32_creditcardrepayments   ,0)                  sum_cnt32_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount7_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount7_creditcardrepayments 
,nvl(max_amount10_creditcardrepayments,0)                  max_amount7_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount7_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt7_creditcardrepayments    
,nvl(sum_amount33_creditcardrepayments,0)                  sum_amount33_creditcardrepayments
,nvl(avg_amount33_creditcardrepayments,0)                  avg_amount33_creditcardrepayments
,nvl(max_amount33_creditcardrepayments,0)                  max_amount33_creditcardrepayments
,nvl(min_amount33_creditcardrepayments,0)                  min_amount33_creditcardrepayments
,nvl(sum_cnt33_creditcardrepayments   ,0)                  sum_cnt33_creditcardrepayments   
,nvl(sum_amount17_enquiryfee          ,0)                  sum_amount17_enquiryfee          
,nvl(avg_amount17_enquiryfee          ,0)                  avg_amount17_enquiryfee          
,nvl(max_amount17_enquiryfee          ,0)                  max_amount17_enquiryfee          
,nvl(sum_cnt17_enquiryfee             ,0)                  sum_cnt17_enquiryfee             
,nvl(min_amount17_enquiryfee          ,0)                  min_amount17_enquiryfee          
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount6_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount6_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount6_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount6_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt6_creditcardrepayments    
,nvl(sum_amount16_enquiryfee          ,0)                  sum_amount16_enquiryfee          
,nvl(avg_amount16_enquiryfee          ,0)                  avg_amount16_enquiryfee          
,nvl(max_amount16_enquiryfee          ,0)                  max_amount16_enquiryfee          
,nvl(sum_cnt16_enquiryfee             ,0)                  sum_cnt16_enquiryfee             
,nvl(min_amount16_enquiryfee          ,0)                  min_amount16_enquiryfee          
,nvl(sum_amount18_enquiryfee          ,0)                  sum_amount18_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)                  avg_amount18_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)                  max_amount18_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)                  sum_cnt18_enquiryfee             
,nvl(min_amount18_enquiryfee          ,0)                  min_amount18_enquiryfee          
,nvl(sum_amount34_creditcardrepayments,0)                  sum_amount34_creditcardrepayments
,nvl(avg_amount34_creditcardrepayments,0)                  avg_amount34_creditcardrepayments
,nvl(max_amount34_creditcardrepayments,0)                  max_amount34_creditcardrepayments
,nvl(min_amount34_creditcardrepayments,0)                  min_amount34_creditcardrepayments
,nvl(sum_cnt34_creditcardrepayments   ,0)                  sum_cnt34_creditcardrepayments   
,nvl(sum_amount15_enquiryfee          ,0)                  sum_amount15_enquiryfee          
,nvl(avg_amount15_enquiryfee          ,0)                  avg_amount15_enquiryfee          
,nvl(max_amount15_enquiryfee          ,0)                  max_amount15_enquiryfee          
,nvl(sum_cnt15_enquiryfee             ,0)                  sum_cnt15_enquiryfee             
,nvl(min_amount15_enquiryfee          ,0)                  min_amount15_enquiryfee          
,nvl(sum_amount19_enquiryfee          ,0)                  sum_amount19_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)                  avg_amount19_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)                  max_amount19_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)                  sum_cnt19_enquiryfee             
,nvl(min_amount19_enquiryfee          ,0)                  min_amount19_enquiryfee          
,nvl(sum_amount20_enquiryfee          ,0)                  sum_amount20_enquiryfee          
,nvl(avg_amount20_enquiryfee          ,0)                  avg_amount20_enquiryfee          
,nvl(max_amount20_enquiryfee          ,0)                  max_amount20_enquiryfee          
,nvl(sum_cnt20_enquiryfee             ,0)                  sum_cnt20_enquiryfee             
,nvl(min_amount20_enquiryfee          ,0)                  min_amount20_enquiryfee          
,nvl(sum_amount14_enquiryfee          ,0)                  sum_amount14_enquiryfee          
,nvl(avg_amount14_enquiryfee          ,0)                  avg_amount14_enquiryfee          
,nvl(max_amount14_enquiryfee          ,0)                  max_amount14_enquiryfee          
,nvl(sum_cnt14_enquiryfee             ,0)                  sum_cnt14_enquiryfee             
,nvl(min_amount14_enquiryfee          ,0)                  min_amount14_enquiryfee          
,nvl(sum_amount21_enquiryfee          ,0)                  sum_amount21_enquiryfee          
,nvl(avg_amount21_enquiryfee          ,0)                  avg_amount21_enquiryfee          
,nvl(max_amount21_enquiryfee          ,0)                  max_amount21_enquiryfee          
,nvl(sum_cnt21_enquiryfee             ,0)                  sum_cnt21_enquiryfee             
,nvl(min_amount21_enquiryfee          ,0)                  min_amount21_enquiryfee          
,nvl(sum_amount13_enquiryfee          ,0)                  sum_amount13_enquiryfee          
,nvl(avg_amount13_enquiryfee          ,0)                  avg_amount13_enquiryfee          
,nvl(max_amount13_enquiryfee          ,0)                  max_amount13_enquiryfee          
,nvl(sum_cnt13_enquiryfee             ,0)                  sum_cnt13_enquiryfee             
,nvl(min_amount13_enquiryfee          ,0)                  min_amount13_enquiryfee          
,nvl(sum_amount22_enquiryfee          ,0)                  sum_amount22_enquiryfee          
,nvl(avg_amount22_enquiryfee          ,0)                  avg_amount22_enquiryfee          
,nvl(max_amount22_enquiryfee          ,0)                  max_amount22_enquiryfee          
,nvl(sum_cnt22_enquiryfee             ,0)                  sum_cnt22_enquiryfee             
,nvl(min_amount22_enquiryfee          ,0)                  min_amount22_enquiryfee          
,nvl(sum_amount12_enquiryfee          ,0)                  sum_amount12_enquiryfee          
,nvl(avg_amount12_enquiryfee          ,0)                  avg_amount12_enquiryfee          
,nvl(max_amount12_enquiryfee          ,0)                  max_amount12_enquiryfee          
,nvl(min_amount12_enquiryfee          ,0)                  min_amount12_enquiryfee          
,nvl(sum_cnt12_enquiryfee             ,0)                  sum_cnt12_enquiryfee             
,nvl(sum_amount35_creditcardrepayments,0)                  sum_amount35_creditcardrepayments
,nvl(avg_amount35_creditcardrepayments,0)                  avg_amount35_creditcardrepayments
,nvl(max_amount35_creditcardrepayments,0)                  max_amount35_creditcardrepayments
,nvl(min_amount35_creditcardrepayments,0)                  min_amount35_creditcardrepayments
,nvl(sum_cnt35_creditcardrepayments   ,0)                  sum_cnt35_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount5_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount5_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount5_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount5_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt5_creditcardrepayments    
,nvl(sum_amount23_enquiryfee          ,0)                  sum_amount23_enquiryfee          
,nvl(avg_amount23_enquiryfee          ,0)                  avg_amount23_enquiryfee          
,nvl(max_amount23_enquiryfee          ,0)                  max_amount23_enquiryfee          
,nvl(sum_cnt23_enquiryfee             ,0)                  sum_cnt23_enquiryfee             
,nvl(min_amount23_enquiryfee          ,0)                  min_amount23_enquiryfee          
,nvl(sum_amount11_enquiryfee          ,0)                  sum_amount11_enquiryfee          
,nvl(avg_amount11_enquiryfee          ,0)                  avg_amount11_enquiryfee          
,nvl(max_amount11_enquiryfee          ,0)                  max_amount11_enquiryfee          
,nvl(min_amount11_enquiryfee          ,0)                  min_amount11_enquiryfee          
,nvl(sum_cnt11_enquiryfee             ,0)                  sum_cnt11_enquiryfee             
,nvl(sum_amount24_enquiryfee          ,0)                  sum_amount24_enquiryfee          
,nvl(avg_amount24_enquiryfee          ,0)                  avg_amount24_enquiryfee          
,nvl(max_amount24_enquiryfee          ,0)                  max_amount24_enquiryfee          
,nvl(sum_cnt24_enquiryfee             ,0)                  sum_cnt24_enquiryfee             
,nvl(min_amount24_enquiryfee          ,0)                  min_amount24_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount10_enquiryfee          
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount10_enquiryfee          
,nvl(max_amount10_enquiryfee          ,0)                  max_amount10_enquiryfee          
,nvl(sum_cnt10_enquiryfee             ,0)                  sum_cnt10_enquiryfee             
,nvl(min_amount10_enquiryfee          ,0)                  min_amount10_enquiryfee          
,nvl(sum_amount36_creditcardrepayments,0)                  sum_amount36_creditcardrepayments
,nvl(avg_amount36_creditcardrepayments,0)                  avg_amount36_creditcardrepayments
,nvl(max_amount36_creditcardrepayments,0)                  max_amount36_creditcardrepayments
,nvl(min_amount36_creditcardrepayments,0)                  min_amount36_creditcardrepayments
,nvl(sum_cnt36_creditcardrepayments   ,0)                  sum_cnt36_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount4_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount4_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount4_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount4_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt4_creditcardrepayments    
,nvl(sum_amount9_enquiryfee           ,0)                  sum_amount9_enquiryfee           
,nvl(avg_amount9_enquiryfee           ,0)                  avg_amount9_enquiryfee           
,nvl(max_amount9_enquiryfee           ,0)                  max_amount9_enquiryfee           
,nvl(sum_cnt9_enquiryfee              ,0)                sum_cnt9_enquiryfee              
,nvl(min_amount9_enquiryfee           ,0)                  min_amount9_enquiryfee           
,nvl(sum_amount25_enquiryfee          ,0)                  sum_amount25_enquiryfee          
,nvl(avg_amount25_enquiryfee          ,0)                  avg_amount25_enquiryfee          
,nvl(max_amount25_enquiryfee          ,0)                  max_amount25_enquiryfee          
,nvl(sum_cnt25_enquiryfee             ,0)                  sum_cnt25_enquiryfee             
,nvl(min_amount25_enquiryfee          ,0)                  min_amount25_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount8_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount8_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount8_enquiryfee           
,nvl(sum_cnt10_enquiryfee             ,0)                  sum_cnt8_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount8_enquiryfee           
,nvl(sum_amount37_creditcardrepayments,0)                  sum_amount37_creditcardrepayments
,nvl(avg_amount37_creditcardrepayments,0)                  avg_amount37_creditcardrepayments
,nvl(max_amount37_creditcardrepayments,0)                  max_amount37_creditcardrepayments
,nvl(min_amount37_creditcardrepayments,0)                  min_amount37_creditcardrepayments
,nvl(sum_cnt37_creditcardrepayments   ,0)                  sum_cnt37_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount3_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount3_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount3_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount3_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt3_creditcardrepayments    
,nvl(sum_amount26_enquiryfee          ,0)                  sum_amount26_enquiryfee          
,nvl(avg_amount26_enquiryfee          ,0)                  avg_amount26_enquiryfee          
,nvl(max_amount26_enquiryfee          ,0)                  max_amount26_enquiryfee          
,nvl(sum_cnt26_enquiryfee             ,0)                  sum_cnt26_enquiryfee             
,nvl(min_amount26_enquiryfee          ,0)                  min_amount26_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount7_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount7_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount7_enquiryfee           
,nvl(sum_cnt10_enquiryfee             ,0)                 sum_cnt7_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount7_enquiryfee           
,nvl(sum_amount27_enquiryfee          ,0)                  sum_amount27_enquiryfee          
,nvl(avg_amount27_enquiryfee          ,0)                  avg_amount27_enquiryfee          
,nvl(max_amount27_enquiryfee          ,0)                  max_amount27_enquiryfee          
,nvl(sum_cnt27_enquiryfee             ,0)                  sum_cnt27_enquiryfee             
,nvl(min_amount27_enquiryfee          ,0)                  min_amount27_enquiryfee          
,nvl(sum_amount38_creditcardrepayments,0)                  sum_amount38_creditcardrepayments
,nvl(avg_amount38_creditcardrepayments,0)                  avg_amount38_creditcardrepayments
,nvl(max_amount38_creditcardrepayments,0)                  max_amount38_creditcardrepayments
,nvl(min_amount38_creditcardrepayments,0)                  min_amount38_creditcardrepayments
,nvl(sum_cnt38_creditcardrepayments   ,0)                  sum_cnt38_creditcardrepayments   
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount6_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount6_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount6_enquiryfee           
,nvl(sum_cnt6_enquiryfee              ,0)                  sum_cnt6_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount6_enquiryfee           
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount2_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount2_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount2_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount2_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt2_creditcardrepayments    
,nvl(sum_amount10_creditcardrepayments,0)                  sum_amount1_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)                  avg_amount1_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)                  max_amount1_creditcardrepayments 
,nvl(min_amount10_creditcardrepayments,0)                  min_amount1_creditcardrepayments 
,nvl(sum_cnt10_creditcardrepayments   ,0)                  sum_cnt1_creditcardrepayments    
,nvl(sum_amount28_enquiryfee          ,0)                  sum_amount28_enquiryfee          
,nvl(avg_amount28_enquiryfee          ,0)                  avg_amount28_enquiryfee          
,nvl(max_amount28_enquiryfee          ,0)                  max_amount28_enquiryfee          
,nvl(sum_cnt28_enquiryfee             ,0)                  sum_cnt28_enquiryfee             
,nvl(min_amount28_enquiryfee          ,0)                  min_amount28_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount5_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount5_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount5_enquiryfee           
,nvl(sum_cnt5_enquiryfee              ,0)                  sum_cnt5_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount5_enquiryfee           
,nvl(sum_amount39_creditcardrepayments,0)                  sum_amount39_creditcardrepayments
,nvl(avg_amount39_creditcardrepayments,0)                  avg_amount39_creditcardrepayments
,nvl(max_amount39_creditcardrepayments,0)                  max_amount39_creditcardrepayments
,nvl(min_amount39_creditcardrepayments,0)                  min_amount39_creditcardrepayments
,nvl(sum_cnt39_creditcardrepayments   ,0)                  sum_cnt39_creditcardrepayments   
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount4_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount4_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount4_enquiryfee           
,nvl(sum_cnt4_enquiryfee              ,0)                  sum_cnt4_enquiryfee              
,nvl(min_amount6_enquiryfee           ,0)                  min_amount4_enquiryfee           
,nvl(sum_amount29_enquiryfee          ,0)                  sum_amount29_enquiryfee          
,nvl(avg_amount29_enquiryfee          ,0)                  avg_amount29_enquiryfee          
,nvl(max_amount29_enquiryfee          ,0)                  max_amount29_enquiryfee          
,nvl(sum_cnt29_enquiryfee             ,0)                  sum_cnt29_enquiryfee             
,nvl(min_amount29_enquiryfee          ,0)                  min_amount29_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount3_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount3_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount3_enquiryfee           
,nvl(min_amount10_enquiryfee          ,0)                  min_amount3_enquiryfee           
,nvl(sum_cnt3_enquiryfee              ,0)                 sum_cnt3_enquiryfee              
,nvl(sum_amount40_creditcardrepayments,0)                  sum_amount40_creditcardrepayments
,nvl(avg_amount40_creditcardrepayments,0)                  avg_amount40_creditcardrepayments
,nvl(max_amount40_creditcardrepayments,0)                  max_amount40_creditcardrepayments
,nvl(min_amount40_creditcardrepayments,0)                  min_amount40_creditcardrepayments
,nvl(sum_cnt40_creditcardrepayments   ,0)                  sum_cnt40_creditcardrepayments   
,nvl(sum_amount6_credit               ,0)                  sum_amount1_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount1_credit               
,nvl(max_amount6_credit               ,0)                  max_amount1_credit               
,nvl(min_amount6_credit               ,0)                  min_amount1_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt1_credit                  
,nvl(sum_amount30_enquiryfee          ,0)                  sum_amount30_enquiryfee          
,nvl(avg_amount30_enquiryfee          ,0)                  avg_amount30_enquiryfee          
,nvl(max_amount30_enquiryfee          ,0)                  max_amount30_enquiryfee          
,nvl(sum_cnt30_enquiryfee             ,0)                  sum_cnt30_enquiryfee             
,nvl(min_amount30_enquiryfee          ,0)                  min_amount30_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount2_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount2_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount2_enquiryfee           
,nvl(sum_cnt2_enquiryfee              ,0)                  sum_cnt2_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount2_enquiryfee           
,nvl(sum_amount10_enquiryfee          ,0)                  sum_amount1_enquiryfee           
,nvl(avg_amount10_enquiryfee          ,0)                  avg_amount1_enquiryfee           
,nvl(max_amount10_enquiryfee          ,0)                  max_amount1_enquiryfee           
,nvl(sum_cnt1_enquiryfee              ,0)                  sum_cnt1_enquiryfee              
,nvl(min_amount10_enquiryfee          ,0)                  min_amount1_enquiryfee           
,nvl(sum_amount41_creditcardrepayments,0)                  sum_amount41_creditcardrepayments
,nvl(avg_amount41_creditcardrepayments,0)                  avg_amount41_creditcardrepayments
,nvl(max_amount41_creditcardrepayments,0)                  max_amount41_creditcardrepayments
,nvl(min_amount41_creditcardrepayments,0)                  min_amount41_creditcardrepayments
,nvl(sum_cnt41_creditcardrepayments   ,0)                  sum_cnt41_creditcardrepayments   
,nvl(sum_amount6_credit               ,0)                  sum_amount2_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount2_credit               
,nvl(max_amount6_credit               ,0)                  max_amount2_credit               
,nvl(min_amount6_credit               ,0)                  min_amount2_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt2_credit                  
,nvl(sum_amount31_enquiryfee          ,0)                  sum_amount31_enquiryfee          
,nvl(avg_amount31_enquiryfee          ,0)                  avg_amount31_enquiryfee          
,nvl(max_amount31_enquiryfee          ,0)                  max_amount31_enquiryfee          
,nvl(sum_cnt31_enquiryfee             ,0)                  sum_cnt31_enquiryfee             
,nvl(min_amount31_enquiryfee          ,0)                  min_amount31_enquiryfee          
,nvl(sum_amount6_credit               ,0)                  sum_amount3_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount3_credit               
,nvl(max_amount6_credit               ,0)                  max_amount3_credit               
,nvl(min_amount6_credit               ,0)                  min_amount3_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt3_credit                  
,nvl(sum_amount42_creditcardrepayments,0)                  sum_amount42_creditcardrepayments
,nvl(avg_amount42_creditcardrepayments,0)                  avg_amount42_creditcardrepayments
,nvl(max_amount42_creditcardrepayments,0)                  max_amount42_creditcardrepayments
,nvl(min_amount42_creditcardrepayments,0)                  min_amount42_creditcardrepayments
,nvl(sum_cnt42_creditcardrepayments   ,0)                  sum_cnt42_creditcardrepayments   
,nvl(sum_amount6_credit               ,0)                  sum_amount4_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount4_credit               
,nvl(max_amount6_credit               ,0)                  max_amount4_credit               
,nvl(min_amount6_credit               ,0)                  min_amount4_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt4_credit                  
,nvl(sum_amount32_enquiryfee          ,0)                  sum_amount32_enquiryfee          
,nvl(avg_amount32_enquiryfee          ,0)                  avg_amount32_enquiryfee          
,nvl(max_amount32_enquiryfee          ,0)                  max_amount32_enquiryfee          
,nvl(sum_cnt32_enquiryfee             ,0)                  sum_cnt32_enquiryfee             
,nvl(min_amount32_enquiryfee          ,0)                  min_amount32_enquiryfee          
,nvl(sum_amount6_credit               ,0)                  sum_amount5_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount5_credit               
,nvl(max_amount6_credit               ,0)                  max_amount5_credit               
,nvl(min_amount6_credit               ,0)                  min_amount5_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt5_credit                  
,nvl(sum_amount43_creditcardrepayments,0)                  sum_amount43_creditcardrepayments
,nvl(avg_amount43_creditcardrepayments,0)                  avg_amount43_creditcardrepayments
,nvl(max_amount43_creditcardrepayments,0)                  max_amount43_creditcardrepayments
,nvl(min_amount43_creditcardrepayments,0)                  min_amount43_creditcardrepayments
,nvl(sum_cnt43_creditcardrepayments   ,0)                  sum_cnt43_creditcardrepayments   
,nvl(sum_amount6_credit               ,0)                  sum_amount6_credit               
,nvl(avg_amount6_credit               ,0)                  avg_amount6_credit               
,nvl(max_amount6_credit               ,0)                  max_amount6_credit               
,nvl(min_amount6_credit               ,0)                  min_amount6_credit               
,nvl(sum_cnt6_credit                  ,0)                  sum_cnt6_credit                  
,nvl(sum_amount33_enquiryfee          ,0)                  sum_amount33_enquiryfee          
,nvl(avg_amount33_enquiryfee          ,0)                  avg_amount33_enquiryfee          
,nvl(max_amount33_enquiryfee          ,0)                  max_amount33_enquiryfee          
,nvl(sum_cnt33_enquiryfee             ,0)                  sum_cnt33_enquiryfee             
,nvl(min_amount33_enquiryfee          ,0)                  min_amount33_enquiryfee          
,nvl(sum_amount44_creditcardrepayments,0)                  sum_amount44_creditcardrepayments
,nvl(avg_amount44_creditcardrepayments,0)                  avg_amount44_creditcardrepayments
,nvl(max_amount44_creditcardrepayments,0)                  max_amount44_creditcardrepayments
,nvl(min_amount44_creditcardrepayments,0)                  min_amount44_creditcardrepayments
,nvl(sum_cnt44_creditcardrepayments   ,0)                  sum_cnt44_creditcardrepayments   
,nvl(sum_amount34_enquiryfee          ,0)                  sum_amount34_enquiryfee          
,nvl(avg_amount34_enquiryfee          ,0)                  avg_amount34_enquiryfee          
,nvl(max_amount34_enquiryfee          ,0)                  max_amount34_enquiryfee          
,nvl(sum_cnt34_enquiryfee             ,0)                  sum_cnt34_enquiryfee             
,nvl(min_amount34_enquiryfee          ,0)                  min_amount34_enquiryfee          
,nvl(sum_amount45_creditcardrepayments ,0)                 sum_amount45_creditcardrepayments
,nvl(avg_amount45_creditcardrepayments,0)                  avg_amount45_creditcardrepayments
,nvl(max_amount45_creditcardrepayments,0)                  max_amount45_creditcardrepayments
,nvl(min_amount45_creditcardrepayments,0)                  min_amount45_creditcardrepayments
,nvl(sum_cnt45_creditcardrepayments   ,0)                  sum_cnt45_creditcardrepayments   
,nvl(sum_amount7_credit               ,0)                  sum_amount7_credit               
,nvl(avg_amount7_credit               ,0)                  avg_amount7_credit               
,nvl(max_amount7_credit               ,0)                  max_amount7_credit               
,nvl(min_amount7_credit               ,0)                  min_amount7_credit               
,nvl(sum_cnt7_credit                  ,0)                  sum_cnt7_credit                  
,nvl(sum_amount35_enquiryfee          ,0)                  sum_amount35_enquiryfee          
,nvl(avg_amount35_enquiryfee          ,0)                  avg_amount35_enquiryfee          
,nvl(max_amount35_enquiryfee          ,0)                  max_amount35_enquiryfee          
,nvl(sum_cnt35_enquiryfee             ,0)                  sum_cnt35_enquiryfee             
,nvl(min_amount35_enquiryfee          ,0)                  min_amount35_enquiryfee          
,nvl(sum_amount46_creditcardrepayments ,0)                 sum_amount46_creditcardrepayments
,nvl(avg_amount46_creditcardrepayments,0)                  avg_amount46_creditcardrepayments
,nvl(max_amount46_creditcardrepayments,0)                  max_amount46_creditcardrepayments
,nvl(min_amount46_creditcardrepayments,0)                  min_amount46_creditcardrepayments
,nvl(sum_cnt46_creditcardrepayments   ,0)                  sum_cnt46_creditcardrepayments   
,nvl(sum_amount8_credit               ,0)                  sum_amount8_credit               
,nvl(avg_amount8_credit               ,0)                  avg_amount8_credit               
,nvl(max_amount8_credit               ,0)                  max_amount8_credit               
,nvl(min_amount8_credit               ,0)                  min_amount8_credit               
,nvl(sum_cnt8_credit                  ,0)                  sum_cnt8_credit                  
,nvl(sum_amount36_enquiryfee          ,0)                  sum_amount36_enquiryfee          
,nvl(avg_amount36_enquiryfee          ,0)                  avg_amount36_enquiryfee          
,nvl(max_amount36_enquiryfee          ,0)                  max_amount36_enquiryfee          
,nvl(sum_cnt36_enquiryfee             ,0)                  sum_cnt36_enquiryfee             
,nvl(min_amount36_enquiryfee          ,0)                  min_amount36_enquiryfee          
,nvl(sum_amount47_creditcardrepayments,0)                  sum_amount47_creditcardrepayments
,nvl(avg_amount47_creditcardrepayments ,0)                 avg_amount47_creditcardrepayments
,nvl(max_amount47_creditcardrepayments,0)                  max_amount47_creditcardrepayments
,nvl(min_amount47_creditcardrepayments,0)                  min_amount47_creditcardrepayments
,nvl(sum_cnt47_creditcardrepayments   ,0)                  sum_cnt47_creditcardrepayments   
,nvl(sum_amount9_credit               ,0)                  sum_amount9_credit               
,nvl(avg_amount9_credit               ,0)                  avg_amount9_credit               
,nvl(max_amount9_credit               ,0)                  max_amount9_credit               
,nvl(min_amount9_credit               ,0)                  min_amount9_credit               
,nvl(sum_cnt9_credit                  ,0)                  sum_cnt9_credit                  
,nvl(sum_amount37_enquiryfee          ,0)                  sum_amount37_enquiryfee          
,nvl(avg_amount37_enquiryfee          ,0)                  avg_amount37_enquiryfee          
,nvl(max_amount37_enquiryfee          ,0)                  max_amount37_enquiryfee          
,nvl(sum_cnt37_enquiryfee             ,0)                  sum_cnt37_enquiryfee             
,nvl(min_amount37_enquiryfee          ,0)                  min_amount37_enquiryfee          
,nvl(sum_amount48_creditcardrepayments,0)                  sum_amount48_creditcardrepayments
,nvl(avg_amount48_creditcardrepayments,0)                  avg_amount48_creditcardrepayments
,nvl(max_amount48_creditcardrepayments ,0)                 max_amount48_creditcardrepayments
,nvl(min_amount48_creditcardrepayments,0)                  min_amount48_creditcardrepayments
,nvl(sum_cnt48_creditcardrepayments   ,0)                  sum_cnt48_creditcardrepayments   
,nvl(sum_amount38_enquiryfee          ,0)                  sum_amount38_enquiryfee          
,nvl(avg_amount38_enquiryfee          ,0)                  avg_amount38_enquiryfee          
,nvl(max_amount38_enquiryfee          ,0)                  max_amount38_enquiryfee          
,nvl(sum_cnt38_enquiryfee             ,0)                  sum_cnt38_enquiryfee             
,nvl(min_amount38_enquiryfee          ,0)                  min_amount38_enquiryfee          
,nvl(sum_amount10_credit              ,0)                  sum_amount10_credit              
,nvl(avg_amount10_credit              ,0)                  avg_amount10_credit              
,nvl(max_amount10_credit              ,0)                  max_amount10_credit              
,nvl(min_amount10_credit              ,0)                  min_amount10_credit              
,nvl(sum_cnt10_credit                 ,0)                  sum_cnt10_credit                 
,nvl(sum_amount39_enquiryfee          ,0)                  sum_amount39_enquiryfee          
,nvl(avg_amount39_enquiryfee          ,0)                  avg_amount39_enquiryfee          
,nvl(max_amount39_enquiryfee          ,0)                  max_amount39_enquiryfee          
,nvl(sum_cnt39_enquiryfee             ,0)                  sum_cnt39_enquiryfee             
,nvl(min_amount39_enquiryfee          ,0)                  min_amount39_enquiryfee          
,nvl(sum_amount49_creditcardrepayments,0)                  sum_amount49_creditcardrepayments
,nvl(avg_amount49_creditcardrepayments,0)                  avg_amount49_creditcardrepayments
,nvl(max_amount49_creditcardrepayments  ,0)                max_amount49_creditcardrepayments
,nvl(min_amount49_creditcardrepayments,0)                  min_amount49_creditcardrepayments
,nvl(sum_cnt49_creditcardrepayments   ,0)                  sum_cnt49_creditcardrepayments   
,nvl(sum_amount11_credit              ,0)                  sum_amount11_credit              
,nvl(avg_amount11_credit              ,0)                  avg_amount11_credit              
,nvl(max_amount11_credit              ,0)                  max_amount11_credit              
,nvl(min_amount11_credit              ,0)                  min_amount11_credit              
,nvl(sum_cnt11_credit                 ,0)                  sum_cnt11_credit                 
,nvl(sum_amount40_enquiryfee          ,0)                  sum_amount40_enquiryfee          
,nvl(avg_amount40_enquiryfee          ,0)                  avg_amount40_enquiryfee          
,nvl(max_amount40_enquiryfee          ,0)                  max_amount40_enquiryfee          
,nvl(min_amount40_enquiryfee          ,0)                  min_amount40_enquiryfee          
,nvl(sum_cnt40_enquiryfee             ,0)                  sum_cnt40_enquiryfee             
,nvl(sum_amount50_creditcardrepayments,0)                  sum_amount50_creditcardrepayments
,nvl(avg_amount50_creditcardrepayments,0)                  avg_amount50_creditcardrepayments
,nvl(max_amount50_creditcardrepayments ,0)                 max_amount50_creditcardrepayments
,nvl(min_amount50_creditcardrepayments,0)                  min_amount50_creditcardrepayments
,nvl(sum_cnt50_creditcardrepayments   ,0)                  sum_cnt50_creditcardrepayments   
,nvl(sum_amount12_credit              ,0)                  sum_amount12_credit              
,nvl(avg_amount12_credit              ,0)                  avg_amount12_credit              
,nvl(max_amount12_credit              ,0)                  max_amount12_credit              
,nvl(min_amount12_credit              ,0)                  min_amount12_credit              
,nvl(sum_cnt12_credit                 ,0)                  sum_cnt12_credit                 
,nvl(sum_amount41_enquiryfee          ,0)                  sum_amount41_enquiryfee          
,nvl(avg_amount41_enquiryfee          ,0)                  avg_amount41_enquiryfee          
,nvl(max_amount41_enquiryfee          ,0)                  max_amount41_enquiryfee          
,nvl(sum_cnt41_enquiryfee             ,0)                  sum_cnt41_enquiryfee             
,nvl(min_amount41_enquiryfee          ,0)                  min_amount41_enquiryfee          
,nvl(sum_amount51_creditcardrepayments,0)                  sum_amount51_creditcardrepayments
,nvl(avg_amount51_creditcardrepayments,0)                  avg_amount51_creditcardrepayments
,nvl(max_amount51_creditcardrepayments,0)                  max_amount51_creditcardrepayments
,nvl(min_amount51_creditcardrepayments ,0)                 min_amount51_creditcardrepayments
,nvl(sum_cnt51_creditcardrepayments   ,0)                  sum_cnt51_creditcardrepayments   
,nvl(sum_amount42_enquiryfee          ,0)                  sum_amount42_enquiryfee          
,nvl(avg_amount42_enquiryfee          ,0)                  avg_amount42_enquiryfee          
,nvl(max_amount42_enquiryfee          ,0)                  max_amount42_enquiryfee          
,nvl(sum_cnt42_enquiryfee             ,0)                  sum_cnt42_enquiryfee             
,nvl(min_amount42_enquiryfee          ,0)                  min_amount42_enquiryfee          
,nvl(sum_amount52_creditcardrepayments ,0)                sum_amount52_creditcardrepayments
,nvl(avg_amount52_creditcardrepayments ,0)                avg_amount52_creditcardrepayments
,nvl(max_amount52_creditcardrepayments ,0)                max_amount52_creditcardrepayments
,nvl(min_amount52_creditcardrepayments,0)                  min_amount52_creditcardrepayments
,nvl(sum_cnt52_creditcardrepayments   ,0)                  sum_cnt52_creditcardrepayments   
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount28_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount28_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount28_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount28_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt28_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount27_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount27_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount27_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount27_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt27_mobilephonerecharge    
,nvl(sum_amount29_mobilephonerecharge ,0)                  sum_amount29_mobilephonerecharge 
,nvl(avg_amount29_mobilephonerecharge ,0)                  avg_amount29_mobilephonerecharge 
,nvl(max_amount29_mobilephonerecharge ,0)                  max_amount29_mobilephonerecharge 
,nvl(min_amount29_mobilephonerecharge ,0)                  min_amount29_mobilephonerecharge 
,nvl(sum_cnt29_mobilephonerecharge    ,0)                  sum_cnt29_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount26_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount26_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount26_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount26_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt26_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount25_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount25_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount25_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount25_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt25_mobilephonerecharge    
,nvl(sum_amount31_mobilephonerecharge ,0)                  sum_amount31_mobilephonerecharge 
,nvl(avg_amount31_mobilephonerecharge ,0)                  avg_amount31_mobilephonerecharge 
,nvl(max_amount31_mobilephonerecharge ,0)                  max_amount31_mobilephonerecharge 
,nvl(min_amount31_mobilephonerecharge ,0)                  min_amount31_mobilephonerecharge 
,nvl(sum_amount30_mobilephonerecharge ,0)                  sum_cnt31_mobilephonerecharge    
,nvl(avg_amount30_mobilephonerecharge ,0)                  sum_amount30_mobilephonerecharge 
,nvl(avg_amount30_mobilephonerecharge ,0)                  avg_amount30_mobilephonerecharge 
,nvl(max_amount30_mobilephonerecharge ,0)                  max_amount30_mobilephonerecharge 
,nvl(min_amount30_mobilephonerecharge ,0)                  min_amount30_mobilephonerecharge 
,nvl(sum_cnt30_mobilephonerecharge    ,0)                  sum_cnt30_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount24_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount24_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount24_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount24_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt24_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount23_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount23_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount23_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount23_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt23_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount22_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount22_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount22_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount22_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt22_mobilephonerecharge    
,nvl(sum_amount32_mobilephonerecharge ,0)                  sum_amount32_mobilephonerecharge 
,nvl(avg_amount32_mobilephonerecharge ,0)                  avg_amount32_mobilephonerecharge 
,nvl(max_amount32_mobilephonerecharge ,0)                  max_amount32_mobilephonerecharge 
,nvl(min_amount32_mobilephonerecharge ,0)                  min_amount32_mobilephonerecharge 
,nvl(sum_cnt32_mobilephonerecharge    ,0)                  sum_cnt32_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount21_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount21_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount21_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount21_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt21_mobilephonerecharge    
,nvl(sum_amount33_mobilephonerecharge ,0)                  sum_amount33_mobilephonerecharge 
,nvl(avg_amount33_mobilephonerecharge ,0)                  avg_amount33_mobilephonerecharge 
,nvl(max_amount33_mobilephonerecharge ,0)                  max_amount33_mobilephonerecharge 
,nvl(min_amount33_mobilephonerecharge ,0)                  min_amount33_mobilephonerecharge 
,nvl(sum_cnt33_mobilephonerecharge    ,0)                  sum_cnt33_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                   sum_amount20_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount20_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount20_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount20_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt20_mobilephonerecharge    
,nvl(sum_amount34_mobilephonerecharge ,0)                  sum_amount34_mobilephonerecharge 
,nvl(avg_amount34_mobilephonerecharge ,0)                  avg_amount34_mobilephonerecharge 
,nvl(max_amount34_mobilephonerecharge ,0)                  max_amount34_mobilephonerecharge 
,nvl(min_amount34_mobilephonerecharge ,0)                  min_amount34_mobilephonerecharge 
,nvl(sum_cnt34_mobilephonerecharge    ,0)                  sum_cnt34_mobilephonerecharge    
,nvl(sum_amount53_creditcardrepayments,0)                  sum_amount53_creditcardrepayments
,nvl(avg_amount53_creditcardrepayments,0)                  avg_amount53_creditcardrepayments
,nvl(max_amount53_creditcardrepayments,0)                   max_amount53_creditcardrepayments
,nvl(min_amount53_creditcardrepayments,0)                  min_amount53_creditcardrepayments
,nvl(sum_cnt53_creditcardrepayments   ,0)                  sum_cnt53_creditcardrepayments   
,nvl(sum_amount13_credit              ,0)                  sum_amount13_credit              
,nvl(avg_amount13_credit              ,0)                  avg_amount13_credit              
,nvl(max_amount13_credit              ,0)                   max_amount13_credit              
,nvl(min_amount13_credit              ,0)                  min_amount13_credit              
,nvl(sum_cnt13_credit                 ,0)                  sum_cnt13_credit                 
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount19_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount19_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount19_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount19_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt19_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount18_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount18_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount18_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount18_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt18_mobilephonerecharge    
,nvl(sum_amount35_mobilephonerecharge ,0)                  sum_amount35_mobilephonerecharge 
,nvl(avg_amount35_mobilephonerecharge ,0)                  avg_amount35_mobilephonerecharge 
,nvl(max_amount35_mobilephonerecharge ,0)                  max_amount35_mobilephonerecharge 
,nvl(min_amount35_mobilephonerecharge ,0)                  min_amount35_mobilephonerecharge 
,nvl(sum_cnt35_mobilephonerecharge    ,0)                  sum_cnt35_mobilephonerecharge    
,nvl(sum_amount43_enquiryfee          ,0)                  sum_amount43_enquiryfee          
,nvl(avg_amount43_enquiryfee          ,0)                  avg_amount43_enquiryfee          
,nvl(max_amount43_enquiryfee          ,0)                  max_amount43_enquiryfee          
,nvl(sum_cnt43_enquiryfee              ,0)                  sum_cnt43_enquiryfee             
,nvl(min_amount43_enquiryfee          ,0)                  min_amount43_enquiryfee          
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount17_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount17_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount17_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount17_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt17_mobilephonerecharge    
,nvl(sum_amount36_mobilephonerecharge ,0)                  sum_amount36_mobilephonerecharge 
,nvl(avg_amount36_mobilephonerecharge ,0)                  avg_amount36_mobilephonerecharge 
,nvl(max_amount36_mobilephonerecharge ,0)                  max_amount36_mobilephonerecharge 
,nvl(min_amount36_mobilephonerecharge ,0)                  min_amount36_mobilephonerecharge 
,nvl(sum_cnt36_mobilephonerecharge    ,0)                  sum_cnt36_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount16_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount16_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount16_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount16_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt16_mobilephonerecharge    
,nvl(sum_amount37_mobilephonerecharge ,0)                  sum_amount37_mobilephonerecharge 
,nvl(avg_amount37_mobilephonerecharge ,0)                  avg_amount37_mobilephonerecharge 
,nvl(max_amount37_mobilephonerecharge ,0)                  max_amount37_mobilephonerecharge 
,nvl(min_amount37_mobilephonerecharge ,0)                  min_amount37_mobilephonerecharge 
,nvl(sum_cnt37_mobilephonerecharge    ,0)                  sum_cnt37_mobilephonerecharge    
,nvl(sum_amount38_mobilephonerecharge ,0)                  sum_amount38_mobilephonerecharge 
,nvl(avg_amount38_mobilephonerecharge ,0)                  avg_amount38_mobilephonerecharge 
,nvl(max_amount38_mobilephonerecharge ,0)                  max_amount38_mobilephonerecharge 
,nvl(min_amount38_mobilephonerecharge ,0)                  min_amount38_mobilephonerecharge 
,nvl(sum_cnt38_mobilephonerecharge    ,0)                  sum_cnt38_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                  sum_amount15_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount15_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount15_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount15_mobilephonerecharge 
,nvl(sum_cnt15_mobilephonerecharge    ,0)                  sum_cnt15_mobilephonerecharge    
,nvl(sum_amount28_mobilephonerecharge ,0)                   sum_amount14_mobilephonerecharge 
,nvl(avg_amount28_mobilephonerecharge ,0)                  avg_amount14_mobilephonerecharge 
,nvl(max_amount28_mobilephonerecharge ,0)                  max_amount14_mobilephonerecharge 
,nvl(min_amount28_mobilephonerecharge ,0)                  min_amount14_mobilephonerecharge 
,nvl(sum_cnt28_mobilephonerecharge    ,0)                  sum_cnt14_mobilephonerecharge    
,nvl(sum_amount54_creditcardrepayments,0)                  sum_amount54_creditcardrepayments
,nvl(avg_amount54_creditcardrepayments,0)                  avg_amount54_creditcardrepayments
,nvl(max_amount54_creditcardrepayments ,0)                  max_amount54_creditcardrepayments
,nvl(min_amount54_creditcardrepayments,0)                  min_amount54_creditcardrepayments
,nvl(sum_cnt54_creditcardrepayments   ,0)                  sum_cnt54_creditcardrepayments   
,nvl(sum_amount39_mobilephonerecharge ,0)                  sum_amount39_mobilephonerecharge 
,nvl(avg_amount39_mobilephonerecharge ,0)                  avg_amount39_mobilephonerecharge 
,nvl(max_amount39_mobilephonerecharge ,0)                  max_amount39_mobilephonerecharge 
,nvl(min_amount39_mobilephonerecharge ,0)                  min_amount39_mobilephonerecharge 
,nvl(sum_cnt39_mobilephonerecharge    ,0)                  sum_cnt39_mobilephonerecharge    

  
from phone_variable_yfq_tnh_2
;        
         
         
     
drop table phone_variable_tnh_creditcardrepayments_20170724_result;
create table  phone_variable_tnh_creditcardrepayments_20170724_result_2  as
select             
phone  
,round(sum_amount1_credit                           ,2)         sum_amount1_credit                                 
,round(avg_amount1_credit                           ,2)         avg_amount1_credit                               
,round(max_amount1_credit                           ,2)         max_amount1_credit                               
,round(min_amount1_credit                           ,2)         min_amount1_credit                               
,round(sum_cnt1_credit                              ,2)         sum_cnt1_credit                                  
,round(sum_amount1_enquirybalance                   ,2)         sum_amount1_enquirybalance                       
,round(sum_amount2_credit                           ,2)         sum_amount2_credit                               
,round(avg_amount2_credit                           ,2)         avg_amount2_credit                               
,round(max_amount2_credit                           ,2)         max_amount2_credit                               
,round(min_amount2_credit                           ,2)         min_amount2_credit                               
,round(sum_cnt2_credit                              ,2)         sum_cnt2_credit                                  
 round(sum_amount3_credit                           ,2)         sum_amount3_credit                               
,round(avg_amount3_credit                           ,2)         avg_amount3_credit                               
,round(max_amount3_credit                           ,2)         max_amount3_credit                               
,round(min_amount3_credit                           ,2)         min_amount3_credit                               
,round(sum_cnt3_credit                              ,2)         sum_cnt3_credit                                  
,round(sum_amount1_creditcardrepayments             ,2)         sum_amount1_creditcardrepayments                 
,round(avg_amount1_creditcardrepayments             ,2)         avg_amount1_creditcardrepayments                 
,round(max_amount1_creditcardrepayments             ,2)         max_amount1_creditcardrepayments                 
,round(min_amount1_creditcardrepayments             ,2)         min_amount1_creditcardrepayments                 
,round(sum_cnt1_creditcardrepayments                ,2)         sum_cnt1_creditcardrepayments                    
,round(sum_amount1_enquiryfee                       ,2)         sum_amount1_enquiryfee                           
,round(avg_amount1_enquiryfee                       ,2)         avg_amount1_enquiryfee                           
,round(max_amount1_enquiryfee                       ,2)         max_amount1_enquiryfee                           
,round(min_amount1_enquiryfee                       ,2)         min_amount1_enquiryfee                           
,round(sum_cnt1_enquiryfee                          ,2)         sum_cnt1_enquiryfee                              
,round(sum_amount5_creditcardrepayments             ,2)         sum_amount5_creditcardrepayments                 
,round(avg_amount5_creditcardrepayments             ,2)         avg_amount5_creditcardrepayments                 
,round(max_amount5_creditcardrepayments             ,2)         max_amount5_creditcardrepayments                 
,round(min_amount5_creditcardrepayments             ,2)         min_amount5_creditcardrepayments                 
,round(sum_cnt5_creditcardrepayments                ,2)         sum_cnt5_creditcardrepayments                    
,round(sum_amount2_creditcardrepayments             ,2)         sum_amount2_creditcardrepayments                 
,round(avg_amount2_creditcardrepayments             ,2)         avg_amount2_creditcardrepayments                 
,round(max_amount2_creditcardrepayments             ,2)         max_amount2_creditcardrepayments                 
,round(min_amount2_creditcardrepayments             ,2)         min_amount2_creditcardrepayments                 
,round(sum_cnt2_creditcardrepayments                ,2)         sum_cnt2_creditcardrepayments                    
,round(sum_amount4_creditcardrepayments             ,2)         sum_amount4_creditcardrepayments                 
,round(avg_amount4_creditcardrepayments             ,2)         avg_amount4_creditcardrepayments                 
,round(max_amount4_creditcardrepayments             ,2)         max_amount4_creditcardrepayments                 
,round(min_amount4_creditcardrepayments             ,2)         min_amount4_creditcardrepayments                 
,round(sum_cnt4_creditcardrepayments                ,2)         sum_cnt4_creditcardrepayments                    
,round(sum_amount6_creditcardrepayments             ,2)         sum_amount6_creditcardrepayments                 
,round(avg_amount6_creditcardrepayments             ,2)         avg_amount6_creditcardrepayments                 
,round(max_amount6_creditcardrepayments             ,2)         max_amount6_creditcardrepayments                 
,round(min_amount6_creditcardrepayments             ,2)         min_amount6_creditcardrepayments                 
,round(sum_cnt6_creditcardrepayments                ,2)         sum_cnt6_creditcardrepayments                    
,round(sum_amount7_creditcardrepayments             ,2)         sum_amount7_creditcardrepayments                 
,round(avg_amount7_creditcardrepayments             ,2)         avg_amount7_creditcardrepayments                 
,round(max_amount7_creditcardrepayments             ,2)         max_amount7_creditcardrepayments                 
,round(min_amount7_creditcardrepayments             ,2)         min_amount7_creditcardrepayments                 
,round(sum_cnt7_creditcardrepayments                ,2)         sum_cnt7_creditcardrepayments                    
,round(sum_amount9_creditcardrepayments             ,2)         sum_amount9_creditcardrepayments                 
,round(avg_amount9_creditcardrepayments             ,2)         avg_amount9_creditcardrepayments                 
,round(max_amount9_creditcardrepayments             ,2)         max_amount9_creditcardrepayments                 
,round(min_amount9_creditcardrepayments             ,2)         min_amount9_creditcardrepayments                 
,round(sum_cnt9_creditcardrepayments                ,2)         sum_cnt9_creditcardrepayments                    
,round(sum_amount3_creditcardrepayments             ,2)         sum_amount3_creditcardrepayments                 
,round(avg_amount3_creditcardrepayments             ,2)         avg_amount3_creditcardrepayments                 
,round(max_amount3_creditcardrepayments             ,2)         max_amount3_creditcardrepayments                 
,round(min_amount3_creditcardrepayments             ,2)         min_amount3_creditcardrepayments                 
,round(sum_cnt3_creditcardrepayments                ,2)         sum_cnt3_creditcardrepayments                    
,round(sum_amount8_creditcardrepayments             ,2)         sum_amount8_creditcardrepayments                 
,round(avg_amount8_creditcardrepayments             ,2)         avg_amount8_creditcardrepayments                 
,round(max_amount8_creditcardrepayments             ,2)         max_amount8_creditcardrepayments                 
,round(min_amount8_creditcardrepayments             ,2)         min_amount8_creditcardrepayments                 
,round(sum_cnt8_creditcardrepayments                ,2)         sum_cnt8_creditcardrepayments                    
,round(sum_amount10_creditcardrepayments            ,2)         sum_amount10_creditcardrepayments                
,round(avg_amount10_creditcardrepayments            ,2)         avg_amount10_creditcardrepayments                
,round(max_amount10_creditcardrepayments            ,2)         max_amount10_creditcardrepayments                
,round(min_amount10_creditcardrepayments            ,2)         min_amount10_creditcardrepayments                
,round(sum_cnt10_creditcardrepayments               ,2)         sum_cnt10_creditcardrepayments                   
,round(sum_amount11_creditcardrepayments            ,2)         sum_amount11_creditcardrepayments                
,round(avg_amount11_creditcardrepayments            ,2)         avg_amount11_creditcardrepayments                
,round(max_amount11_creditcardrepayments            ,2)         max_amount11_creditcardrepayments                
,round(min_amount11_creditcardrepayments            ,2)         min_amount11_creditcardrepayments                
,round(sum_cnt11_creditcardrepayments               ,2)         sum_cnt11_creditcardrepayments                   
,round(sum_amount12_creditcardrepayments            ,2)         sum_amount12_creditcardrepayments                
,round(avg_amount12_creditcardrepayments            ,2)         avg_amount12_creditcardrepayments                
,round(max_amount12_creditcardrepayments            ,2)         max_amount12_creditcardrepayments                
,round(min_amount12_creditcardrepayments            ,2)         min_amount12_creditcardrepayments                
,round(sum_cnt12_creditcardrepayments               ,2)         sum_cnt12_creditcardrepayments                   
,round(sum_amount2_enquiryfee                       ,2)         sum_amount2_enquiryfee                           
,round(avg_amount2_enquiryfee                       ,2)         avg_amount2_enquiryfee                           
,round(max_amount2_enquiryfee                       ,2)         max_amount2_enquiryfee                           
,round(min_amount2_enquiryfee                       ,2)         min_amount2_enquiryfee                           
,round(sum_cnt2_enquiryfee                          ,2)         sum_cnt2_enquiryfee                              
,round(sum_amount14_creditcardrepayments            ,2)         sum_amount14_creditcardrepayments                
,round(avg_amount14_creditcardrepayments            ,2)         avg_amount14_creditcardrepayments                
,round(max_amount14_creditcardrepayments            ,2)         max_amount14_creditcardrepayments                
,round(min_amount14_creditcardrepayments            ,2)         min_amount14_creditcardrepayments                
,round(sum_cnt14_creditcardrepayments               ,2)         sum_cnt14_creditcardrepayments                   
,round(sum_amount13_creditcardrepayments            ,2)         sum_amount13_creditcardrepayments                
,round(avg_amount13_creditcardrepayments            ,2)         avg_amount13_creditcardrepayments                
,round(max_amount13_creditcardrepayments            ,2)         max_amount13_creditcardrepayments                
,round(min_amount13_creditcardrepayments            ,2)         min_amount13_creditcardrepayments                
,round(sum_cnt13_creditcardrepayments               ,2)         sum_cnt13_creditcardrepayments                   
,round(sum_amount15_creditcardrepayments            ,2)         sum_amount15_creditcardrepayments                
,round(avg_amount15_creditcardrepayments            ,2)         avg_amount15_creditcardrepayments                
,round(max_amount15_creditcardrepayments            ,2)         max_amount15_creditcardrepayments                
,round(min_amount15_creditcardrepayments            ,2)         min_amount15_creditcardrepayments                
,round(sum_cnt15_creditcardrepayments               ,2)         sum_cnt15_creditcardrepayments                   
,round(sum_amount16_creditcardrepayments            ,2)         sum_amount16_creditcardrepayments                
,round(avg_amount16_creditcardrepayments            ,2)         avg_amount16_creditcardrepayments                
,round(max_amount16_creditcardrepayments            ,2)         max_amount16_creditcardrepayments                
,round(min_amount16_creditcardrepayments            ,2)         min_amount16_creditcardrepayments                
,round(sum_cnt16_creditcardrepayments               ,2)         sum_cnt16_creditcardrepayments                   
,round(sum_amount17_creditcardrepayments            ,2)         sum_amount17_creditcardrepayments                
,round(avg_amount17_creditcardrepayments            ,2)         avg_amount17_creditcardrepayments                
,round(max_amount17_creditcardrepayments            ,2)         max_amount17_creditcardrepayments                
,round(min_amount17_creditcardrepayments            ,2)         min_amount17_creditcardrepayments                
,round(sum_cnt17_creditcardrepayments               ,2)         sum_cnt17_creditcardrepayments                   
,round(sum_amount3_enquiryfee                       ,2)         sum_amount3_enquiryfee                           
,round(avg_amount3_enquiryfee                       ,2)         avg_amount3_enquiryfee                           
,round(max_amount3_enquiryfee                       ,2)         max_amount3_enquiryfee                           
,round(min_amount3_enquiryfee                       ,2)         min_amount3_enquiryfee                           
,round(sum_cnt3_enquiryfee                          ,2)         sum_cnt3_enquiryfee                              
,round(sum_amount18_creditcardrepayments            ,2)         sum_amount18_creditcardrepayments                
,round(avg_amount18_creditcardrepayments            ,2)         avg_amount18_creditcardrepayments                
,round(max_amount18_creditcardrepayments            ,2)         max_amount18_creditcardrepayments                
,round(min_amount18_creditcardrepayments            ,2)         min_amount18_creditcardrepayments                
,round(sum_cnt18_creditcardrepayments               ,2)         sum_cnt18_creditcardrepayments                   
,round(sum_amount4_credit                           ,2)         sum_amount4_credit                               
,round(avg_amount4_credit                           ,2)         avg_amount4_credit                               
,round(max_amount4_credit                           ,2)         max_amount4_credit                               
,round(min_amount4_credit                           ,2)         min_amount4_credit                               
,round(sum_cnt4_credit                              ,2)         sum_cnt4_credit                                  
,round(sum_amount19_creditcardrepayments            ,2)         sum_amount19_creditcardrepayments                
,round(avg_amount19_creditcardrepayments            ,2)         avg_amount19_creditcardrepayments                
,round(max_amount19_creditcardrepayments            ,2)         max_amount19_creditcardrepayments                
,round(min_amount19_creditcardrepayments            ,2)         min_amount19_creditcardrepayments                
,round(sum_cnt19_creditcardrepayments               ,2)         sum_cnt19_creditcardrepayments                   
,round(sum_amount4_enquiryfee                       ,2)         sum_amount4_enquiryfee                           
,round(avg_amount4_enquiryfee                       ,2)         avg_amount4_enquiryfee                           
,round(max_amount4_enquiryfee                       ,2)         max_amount4_enquiryfee                           
,round(min_amount4_enquiryfee                       ,2)         min_amount4_enquiryfee                           
,round(sum_cnt4_enquiryfee                          ,2)         sum_cnt4_enquiryfee                              
,round(sum_amount20_creditcardrepayments            ,2)         sum_amount20_creditcardrepayments                
,round(avg_amount20_creditcardrepayments            ,2)         avg_amount20_creditcardrepayments                
,round(max_amount20_creditcardrepayments            ,2)         max_amount20_creditcardrepayments                
,round(min_amount20_creditcardrepayments            ,2)         min_amount20_creditcardrepayments                
,round(sum_cnt20_creditcardrepayments               ,2)         sum_cnt20_creditcardrepayments                   
,round(sum_amount5_enquiryfee                       ,2)         sum_amount5_enquiryfee                           
,round(avg_amount5_enquiryfee                       ,2)         avg_amount5_enquiryfee                           
,round(max_amount5_enquiryfee                       ,2)         max_amount5_enquiryfee                           
,round(min_amount5_enquiryfee                       ,2)         min_amount5_enquiryfee                           
,round(sum_cnt5_enquiryfee                          ,2)         sum_cnt5_enquiryfee                              
,round(sum_amount6_enquiryfee                       ,2)         sum_amount6_enquiryfee                           
,round(avg_amount6_enquiryfee                       ,2)         avg_amount6_enquiryfee                           
,round(max_amount6_enquiryfee                       ,2)         max_amount6_enquiryfee                           
,round(min_amount6_enquiryfee                       ,2)         min_amount6_enquiryfee                           
,round(sum_cnt6_enquiryfee                          ,2)         sum_cnt6_enquiryfee                              
,round(sum_amount21_creditcardrepayments            ,2)         sum_amount21_creditcardrepayments                
,round(avg_amount21_creditcardrepayments            ,2)         avg_amount21_creditcardrepayments                
,round(max_amount21_creditcardrepayments            ,2)         max_amount21_creditcardrepayments                
,round(min_amount21_creditcardrepayments            ,2)         min_amount21_creditcardrepayments                
,round(sum_cnt21_creditcardrepayments               ,2)         sum_cnt21_creditcardrepayments                   
,round(sum_amount7_enquiryfee                       ,2)         sum_amount7_enquiryfee                           
,round(avg_amount7_enquiryfee                       ,2)         avg_amount7_enquiryfee                           
,round(max_amount7_enquiryfee                       ,2)         max_amount7_enquiryfee                           
,round(sum_cnt7_enquiryfee                          ,2)         sum_cnt7_enquiryfee                              
,round(min_amount7_enquiryfee                       ,2)         min_amount7_enquiryfee                           
,round(sum_amount22_creditcardrepayments            ,2)         sum_amount22_creditcardrepayments                
,round(avg_amount22_creditcardrepayments            ,2)         avg_amount22_creditcardrepayments                
,round(max_amount22_creditcardrepayments            ,2)         max_amount22_creditcardrepayments                
,round(min_amount22_creditcardrepayments            ,2)         min_amount22_creditcardrepayments                
,round(sum_cnt22_creditcardrepayments               ,2)         sum_cnt22_creditcardrepayments                   
,round(sum_amount8_enquiryfee                       ,2)         sum_amount8_enquiryfee                           
,round(avg_amount8_enquiryfee                       ,2)         avg_amount8_enquiryfee                           
,round(max_amount8_enquiryfee                       ,2)         max_amount8_enquiryfee                           
,round(sum_cnt8_enquiryfee                          ,2)         sum_cnt8_enquiryfee                              
,round(min_amount8_enquiryfee                       ,2)         min_amount8_enquiryfee                           
,round(sum_amount23_creditcardrepayments            ,2)         sum_amount23_creditcardrepayments                
,round(avg_amount23_creditcardrepayments            ,2)         avg_amount23_creditcardrepayments                
,round(max_amount23_creditcardrepayments            ,2)         max_amount23_creditcardrepayments                
,round(min_amount23_creditcardrepayments            ,2)         min_amount23_creditcardrepayments                
,round(sum_cnt23_creditcardrepayments               ,2)         sum_cnt23_creditcardrepayments                   
,round(sum_amount9_enquiryfee                       ,2)         sum_amount9_enquiryfee                           
,round(avg_amount9_enquiryfee                       ,2)         avg_amount9_enquiryfee                           
,round(max_amount9_enquiryfee                       ,2)         max_amount9_enquiryfee                           
,round(min_amount9_enquiryfee                       ,2)         min_amount9_enquiryfee                           
,round(sum_cnt9_enquiryfee                          ,2)         sum_cnt9_enquiryfee                              
,round(sum_amount24_creditcardrepayments            ,2)         sum_amount24_creditcardrepayments                
,round(avg_amount24_creditcardrepayments            ,2)         avg_amount24_creditcardrepayments                
,round(max_amount24_creditcardrepayments            ,2)         max_amount24_creditcardrepayments                
,round(min_amount24_creditcardrepayments            ,2)         min_amount24_creditcardrepayments                
,round(sum_cnt24_creditcardrepayments               ,2)         sum_cnt24_creditcardrepayments                   
,round(sum_amount10_enquiryfee                      ,2)         sum_amount10_enquiryfee                          
,round(avg_amount10_enquiryfee                      ,2)         avg_amount10_enquiryfee                          
,round(max_amount10_enquiryfee                      ,2)         max_amount10_enquiryfee                          
,round(min_amount10_enquiryfee                      ,2)         min_amount10_enquiryfee                          
,round(sum_cnt10_enquiryfee                         ,2)         sum_cnt10_enquiryfee                             
,round(sum_amount25_creditcardrepayments            ,2)         sum_amount25_creditcardrepayments                
,round(avg_amount25_creditcardrepayments            ,2)         avg_amount25_creditcardrepayments                
,round(max_amount25_creditcardrepayments            ,2)         max_amount25_creditcardrepayments                
,round(min_amount25_creditcardrepayments            ,2)         min_amount25_creditcardrepayments                
,round(sum_cnt25_creditcardrepayments               ,2)         sum_cnt25_creditcardrepayments                   
,round(sum_amount11_enquiryfee                      ,2)         sum_amount11_enquiryfee                          
,round(avg_amount11_enquiryfee                      ,2)         avg_amount11_enquiryfee                          
,round(max_amount11_enquiryfee                      ,2)         max_amount11_enquiryfee                          
,round(min_amount11_enquiryfee                      ,2)         min_amount11_enquiryfee                          
,round(sum_cnt11_enquiryfee                         ,2)         sum_cnt11_enquiryfee                             
,round(sum_amount26_creditcardrepayments            ,2)         sum_amount26_creditcardrepayments                
,round(avg_amount26_creditcardrepayments            ,2)         avg_amount26_creditcardrepayments                
,round(max_amount26_creditcardrepayments            ,2)         max_amount26_creditcardrepayments                
,round(min_amount26_creditcardrepayments            ,2)         min_amount26_creditcardrepayments                
,round(sum_cnt26_creditcardrepayments               ,2)         sum_cnt26_creditcardrepayments                   
,round(sum_amount12_enquiryfee                      ,2)         sum_amount12_enquiryfee                          
,round(avg_amount12_enquiryfee                      ,2)         avg_amount12_enquiryfee                          
,round(max_amount12_enquiryfee                      ,2)         max_amount12_enquiryfee                          
,round(min_amount12_enquiryfee                      ,2)         min_amount12_enquiryfee                          
,round(sum_cnt12_enquiryfee                         ,2)         sum_cnt12_enquiryfee                             
,round(sum_amount27_creditcardrepayments            ,2)         sum_amount27_creditcardrepayments                
,round(avg_amount27_creditcardrepayments            ,2)         avg_amount27_creditcardrepayments                
,round(max_amount27_creditcardrepayments            ,2)         max_amount27_creditcardrepayments                
,round(min_amount27_creditcardrepayments            ,2)         min_amount27_creditcardrepayments                
,round(sum_cnt27_creditcardrepayments               ,2)         sum_cnt27_creditcardrepayments                   
,round(sum_amount13_enquiryfee                      ,2)         sum_amount13_enquiryfee                          
,round(avg_amount13_enquiryfee                      ,2)         avg_amount13_enquiryfee                          
,round(max_amount13_enquiryfee                      ,2)         max_amount13_enquiryfee                          
,round(min_amount13_enquiryfee                      ,2)         min_amount13_enquiryfee                          
,round(sum_cnt13_enquiryfee                         ,2)         sum_cnt13_enquiryfee                             
,round(sum_amount14_enquiryfee                      ,2)         sum_amount14_enquiryfee                          
,round(avg_amount14_enquiryfee                      ,2)         avg_amount14_enquiryfee                          
,round(max_amount14_enquiryfee                      ,2)         max_amount14_enquiryfee                          
,round(min_amount14_enquiryfee                      ,2)         min_amount14_enquiryfee                          
,round(sum_cnt14_enquiryfee                         ,2)         sum_cnt14_enquiryfee                             
,round(sum_amount28_creditcardrepayments            ,2)         sum_amount28_creditcardrepayments                
,round(avg_amount28_creditcardrepayments            ,2)         avg_amount28_creditcardrepayments                
,round(max_amount28_creditcardrepayments            ,2)         max_amount28_creditcardrepayments                
,round(min_amount28_creditcardrepayments            ,2)         min_amount28_creditcardrepayments                
,round(sum_cnt28_creditcardrepayments               ,2)         sum_cnt28_creditcardrepayments                   
,round(sum_amount29_creditcardrepayments            ,2)         sum_amount29_creditcardrepayments                
,round(avg_amount29_creditcardrepayments            ,2)         avg_amount29_creditcardrepayments                
,round(max_amount29_creditcardrepayments            ,2)         max_amount29_creditcardrepayments                
,round(min_amount29_creditcardrepayments            ,2)         min_amount29_creditcardrepayments                
,round(sum_cnt29_creditcardrepayments               ,2)         sum_cnt29_creditcardrepayments                   
,round(sum_amount15_enquiryfee                      ,2)         sum_amount15_enquiryfee                          
,round(avg_amount15_enquiryfee                      ,2)         avg_amount15_enquiryfee                          
,round(max_amount15_enquiryfee                      ,2)         max_amount15_enquiryfee                          
,round(sum_cnt15_enquiryfee                         ,2)         sum_cnt15_enquiryfee                             
,round(min_amount15_enquiryfee                      ,2)         min_amount15_enquiryfee                          
,round(sum_amount16_enquiryfee                      ,2)         sum_amount16_enquiryfee                          
,round(avg_amount16_enquiryfee                      ,2)         avg_amount16_enquiryfee                          
,round(max_amount16_enquiryfee                      ,2)         max_amount16_enquiryfee                          
,round(min_amount16_enquiryfee                      ,2)         min_amount16_enquiryfee                          
,round(sum_cnt16_enquiryfee                         ,2)         sum_cnt16_enquiryfee                             
,round(sum_amount30_creditcardrepayments            ,2)         sum_amount30_creditcardrepayments                
,round(avg_amount30_creditcardrepayments            ,2)         avg_amount30_creditcardrepayments                
,round(max_amount30_creditcardrepayments            ,2)         max_amount30_creditcardrepayments                
,round(min_amount30_creditcardrepayments            ,2)         min_amount30_creditcardrepayments                
,round(sum_cnt30_creditcardrepayments               ,2)         sum_cnt30_creditcardrepayments                   
,round(sum_amount17_enquiryfee                      ,2)         sum_amount17_enquiryfee                          
,round(avg_amount17_enquiryfee                      ,2)         avg_amount17_enquiryfee                          
,round(max_amount17_enquiryfee                      ,2)         max_amount17_enquiryfee                          
,round(min_amount17_enquiryfee                      ,2)         min_amount17_enquiryfee                          
,round(sum_cnt17_enquiryfee                         ,2)         sum_cnt17_enquiryfee                             
,round(sum_amount5_credit                           ,2)         sum_amount5_credit                               
,round(avg_amount5_credit                           ,2)         avg_amount5_credit                               
,round(max_amount5_credit                           ,2)         max_amount5_credit                               
,round(min_amount5_credit                           ,2)         min_amount5_credit                               
,round(sum_cnt5_credit                              ,2)         sum_cnt5_credit                                  
,round(sum_amount31_creditcardrepayments            ,2)         sum_amount31_creditcardrepayments                
,round(avg_amount31_creditcardrepayments            ,2)         avg_amount31_creditcardrepayments                
,round(max_amount31_creditcardrepayments            ,2)         max_amount31_creditcardrepayments                
,round(min_amount31_creditcardrepayments            ,2)         min_amount31_creditcardrepayments                
,round(sum_cnt31_creditcardrepayments               ,2)         sum_cnt31_creditcardrepayments                   
,round(sum_amount18_enquiryfee                      ,2)         sum_amount18_enquiryfee                          
,round(avg_amount18_enquiryfee                      ,2)         avg_amount18_enquiryfee                          
,round(max_amount18_enquiryfee                      ,2)         max_amount18_enquiryfee                          
,round(sum_cnt18_enquiryfee                         ,2)         sum_cnt18_enquiryfee                             
,round(min_amount18_enquiryfee                      ,2)         min_amount18_enquiryfee                          
,round(sum_amount19_enquiryfee                      ,2)         sum_amount19_enquiryfee                          
,round(avg_amount19_enquiryfee                      ,2)         avg_amount19_enquiryfee                          
,round(max_amount19_enquiryfee                      ,2)         max_amount19_enquiryfee                          
,round(min_amount19_enquiryfee                      ,2)         min_amount19_enquiryfee                          
,round(sum_cnt19_enquiryfee                         ,2)         sum_cnt19_enquiryfee                             
,round(sum_amount32_creditcardrepayments            ,2)         sum_amount32_creditcardrepayments                
,round(avg_amount32_creditcardrepayments            ,2)         avg_amount32_creditcardrepayments                
,round(max_amount32_creditcardrepayments            ,2)         max_amount32_creditcardrepayments                
,round(min_amount32_creditcardrepayments            ,2)         min_amount32_creditcardrepayments                
,round(sum_cnt32_creditcardrepayments               ,2)         sum_cnt32_creditcardrepayments                   
,round(sum_amount20_enquiryfee                      ,2)         sum_amount20_enquiryfee                          
,round(avg_amount20_enquiryfee                      ,2)         avg_amount20_enquiryfee                          
,round(max_amount20_enquiryfee                      ,2)         max_amount20_enquiryfee                          
,round(min_amount20_enquiryfee                      ,2)         min_amount20_enquiryfee                          
,round(sum_cnt20_enquiryfee                         ,2)         sum_cnt20_enquiryfee                             
,round(sum_amount33_creditcardrepayments            ,2)         sum_amount33_creditcardrepayments                
,round(avg_amount33_creditcardrepayments            ,2)         avg_amount33_creditcardrepayments                
,round(max_amount33_creditcardrepayments            ,2)         max_amount33_creditcardrepayments                
,round(min_amount33_creditcardrepayments            ,2)         min_amount33_creditcardrepayments                
,round(sum_cnt33_creditcardrepayments               ,2)         sum_cnt33_creditcardrepayments                   
,round(sum_amount21_enquiryfee                      ,2)         sum_amount21_enquiryfee                          
,round(avg_amount21_enquiryfee                      ,2)         avg_amount21_enquiryfee                          
,round(max_amount21_enquiryfee                      ,2)         max_amount21_enquiryfee                          
,round(sum_cnt21_enquiryfee                         ,2)         sum_cnt21_enquiryfee                             
,round(min_amount21_enquiryfee                      ,2)         min_amount21_enquiryfee                          
,round(sum_amount34_creditcardrepayments            ,2)         sum_amount34_creditcardrepayments                
,round(avg_amount34_creditcardrepayments            ,2)         avg_amount34_creditcardrepayments                
,round(max_amount34_creditcardrepayments            ,2)         max_amount34_creditcardrepayments                
,round(min_amount34_creditcardrepayments            ,2)         min_amount34_creditcardrepayments                
,round(sum_cnt34_creditcardrepayments               ,2)         sum_cnt34_creditcardrepayments                   
,round(sum_amount22_enquiryfee                      ,2)         sum_amount22_enquiryfee                          
,round(avg_amount22_enquiryfee                      ,2)         avg_amount22_enquiryfee                          
,round(max_amount22_enquiryfee                      ,2)         max_amount22_enquiryfee                          
,round(min_amount22_enquiryfee                      ,2)         min_amount22_enquiryfee                          
,round(sum_cnt22_enquiryfee                         ,2)         sum_cnt22_enquiryfee                             
,round(sum_amount35_creditcardrepayments            ,2)         sum_amount35_creditcardrepayments                
,round(avg_amount35_creditcardrepayments            ,2)         avg_amount35_creditcardrepayments                
,round(max_amount35_creditcardrepayments            ,2)         max_amount35_creditcardrepayments                
,round(min_amount35_creditcardrepayments            ,2)         min_amount35_creditcardrepayments                
,round(sum_cnt35_creditcardrepayments               ,2)         sum_cnt35_creditcardrepayments                   
,round(sum_amount23_enquiryfee                      ,2)         sum_amount23_enquiryfee                          
,round(avg_amount23_enquiryfee                      ,2)         avg_amount23_enquiryfee                          
,round(max_amount23_enquiryfee                      ,2)         max_amount23_enquiryfee                          
,round(min_amount23_enquiryfee                      ,2)         min_amount23_enquiryfee                          
,round(sum_cnt23_enquiryfee                         ,2)         sum_cnt23_enquiryfee                             
,round(sum_amount6_credit                           ,2)         sum_amount6_credit                               
,round(avg_amount6_credit                           ,2)         avg_amount6_credit                               
,round(max_amount6_credit                           ,2)         max_amount6_credit                               
,round(min_amount6_credit                           ,2)         min_amount6_credit                               
,round(sum_cnt6_credit                              ,2)         sum_cnt6_credit                                  
,round(sum_amount36_creditcardrepayments            ,2)         sum_amount36_creditcardrepayments                
,round(avg_amount36_creditcardrepayments            ,2)         avg_amount36_creditcardrepayments                
,round(max_amount36_creditcardrepayments            ,2)         max_amount36_creditcardrepayments                
,round(min_amount36_creditcardrepayments            ,2)         min_amount36_creditcardrepayments                
,round(sum_cnt36_creditcardrepayments               ,2)         sum_cnt36_creditcardrepayments                   
,round(sum_amount24_enquiryfee                      ,2)         sum_amount24_enquiryfee                          
,round(avg_amount24_enquiryfee                      ,2)         avg_amount24_enquiryfee                          
,round(max_amount24_enquiryfee                      ,2)         max_amount24_enquiryfee                          
,round(min_amount24_enquiryfee                      ,2)         min_amount24_enquiryfee                          
,round(sum_cnt24_enquiryfee                         ,2)         sum_cnt24_enquiryfee                             
,round(sum_amount25_enquiryfee                      ,2)         sum_amount25_enquiryfee                          
,round(avg_amount25_enquiryfee                      ,2)         avg_amount25_enquiryfee                          
,round(max_amount25_enquiryfee                      ,2)         max_amount25_enquiryfee                          
,round(sum_cnt25_enquiryfee                         ,2)         sum_cnt25_enquiryfee                             
,round(min_amount25_enquiryfee                      ,2)         min_amount25_enquiryfee                          
,round(sum_amount37_creditcardrepayments            ,2)         sum_amount37_creditcardrepayments                
,round(avg_amount37_creditcardrepayments            ,2)         avg_amount37_creditcardrepayments                
,round(max_amount37_creditcardrepayments            ,2)         max_amount37_creditcardrepayments                
,round(min_amount37_creditcardrepayments            ,2)         min_amount37_creditcardrepayments                
,round(sum_cnt37_creditcardrepayments               ,2)         sum_cnt37_creditcardrepayments                   
,round(sum_amount26_enquiryfee                      ,2)         sum_amount26_enquiryfee                          
,round(avg_amount26_enquiryfee                      ,2)         avg_amount26_enquiryfee                          
,round(max_amount26_enquiryfee                      ,2)         max_amount26_enquiryfee                          
,round(min_amount26_enquiryfee                      ,2)         min_amount26_enquiryfee                          
,round(sum_cnt26_enquiryfee                         ,2)         sum_cnt26_enquiryfee                             
,round(sum_amount38_creditcardrepayments            ,2)         sum_amount38_creditcardrepayments                
,round(avg_amount38_creditcardrepayments            ,2)         avg_amount38_creditcardrepayments                
,round(max_amount38_creditcardrepayments            ,2)         max_amount38_creditcardrepayments                
,round(min_amount38_creditcardrepayments            ,2)         min_amount38_creditcardrepayments                
,round(sum_cnt38_creditcardrepayments               ,2)         sum_cnt38_creditcardrepayments                   
,round(sum_amount7_credit                           ,2)         sum_amount7_credit                               
,round(avg_amount7_credit                           ,2)         avg_amount7_credit                               
,round(max_amount7_credit                           ,2)         max_amount7_credit                               
,round(min_amount7_credit                           ,2)         min_amount7_credit                               
,round(sum_cnt7_credit                              ,2)         sum_cnt7_credit                                  
,round(sum_amount27_enquiryfee                      ,2)         sum_amount27_enquiryfee                          
,round(avg_amount27_enquiryfee                      ,2)         avg_amount27_enquiryfee                          
,round(max_amount27_enquiryfee                      ,2)         max_amount27_enquiryfee                          
,round(min_amount27_enquiryfee                      ,2)         min_amount27_enquiryfee                          
,round(sum_cnt27_enquiryfee                         ,2)         sum_cnt27_enquiryfee                             
,round(sum_amount39_creditcardrepayments            ,2)         sum_amount39_creditcardrepayments                
,round(avg_amount39_creditcardrepayments            ,2)         avg_amount39_creditcardrepayments                
,round(max_amount39_creditcardrepayments            ,2)         max_amount39_creditcardrepayments                
,round(min_amount39_creditcardrepayments            ,2)         min_amount39_creditcardrepayments                
,round(sum_cnt39_creditcardrepayments               ,2)         sum_cnt39_creditcardrepayments                   
,round(sum_amount28_enquiryfee                      ,2)         sum_amount28_enquiryfee                          
,round(avg_amount28_enquiryfee                      ,2)         avg_amount28_enquiryfee                          
,round(max_amount28_enquiryfee                      ,2)         max_amount28_enquiryfee                          
,round(min_amount28_enquiryfee                      ,2)         min_amount28_enquiryfee                          
,round(sum_cnt28_enquiryfee                         ,2)         sum_cnt28_enquiryfee                             
,round(sum_amount40_creditcardrepayments            ,2)         sum_amount40_creditcardrepayments                
,round(avg_amount40_creditcardrepayments            ,2)         avg_amount40_creditcardrepayments                
,round(max_amount40_creditcardrepayments            ,2)         max_amount40_creditcardrepayments                
,round(min_amount40_creditcardrepayments            ,2)         min_amount40_creditcardrepayments                
,round(sum_cnt40_creditcardrepayments               ,2)         sum_cnt40_creditcardrepayments                   
,round(sum_amount29_enquiryfee                      ,2)         sum_amount29_enquiryfee                          
,round(avg_amount29_enquiryfee                      ,2)         avg_amount29_enquiryfee                          
,round(max_amount29_enquiryfee                      ,2)         max_amount29_enquiryfee                          
,round(min_amount29_enquiryfee                      ,2)         min_amount29_enquiryfee                          
,round(sum_cnt29_enquiryfee                         ,2)         sum_cnt29_enquiryfee                             
,round(sum_amount8_credit                           ,2)         sum_amount8_credit                               
,round(avg_amount8_credit                           ,2)         avg_amount8_credit                               
,round(max_amount8_credit                           ,2)         max_amount8_credit                               
,round(min_amount8_credit                           ,2)         min_amount8_credit                               
,round(sum_cnt8_credit                              ,2)         sum_cnt8_credit                                  
,round(sum_amount41_creditcardrepayments            ,2)         sum_amount41_creditcardrepayments                
,round(avg_amount41_creditcardrepayments            ,2)         avg_amount41_creditcardrepayments                
,round(max_amount41_creditcardrepayments            ,2)         max_amount41_creditcardrepayments                
,round(min_amount41_creditcardrepayments            ,2)         min_amount41_creditcardrepayments                
,round(sum_cnt41_creditcardrepayments               ,2)         sum_cnt41_creditcardrepayments                   
,round(sum_amount30_enquiryfee                      ,2)         sum_amount30_enquiryfee                          
,round(avg_amount30_enquiryfee                      ,2)         avg_amount30_enquiryfee                          
,round(max_amount30_enquiryfee                      ,2)         max_amount30_enquiryfee                          
,round(min_amount30_enquiryfee                      ,2)         min_amount30_enquiryfee                          
,round(sum_cnt30_enquiryfee                         ,2)         sum_cnt30_enquiryfee                             
,round(sum_amount42_creditcardrepayments            ,2)         sum_amount42_creditcardrepayments                
,round(avg_amount42_creditcardrepayments            ,2)         avg_amount42_creditcardrepayments                
,round(max_amount42_creditcardrepayments            ,2)         max_amount42_creditcardrepayments                
,round(min_amount42_creditcardrepayments            ,2)         min_amount42_creditcardrepayments                
,round(sum_cnt42_creditcardrepayments               ,2)         sum_cnt42_creditcardrepayments                   
,round(sum_amount31_enquiryfee                      ,2)         sum_amount31_enquiryfee                          
,round(avg_amount31_enquiryfee                      ,2)         avg_amount31_enquiryfee                          
,round(max_amount31_enquiryfee                      ,2)         max_amount31_enquiryfee                          
,round(min_amount31_enquiryfee                      ,2)         min_amount31_enquiryfee                          
,round(sum_cnt31_enquiryfee                         ,2)         sum_cnt31_enquiryfee                             
,round(sum_amount9_credit                           ,2)         sum_amount9_credit                               
,round(avg_amount9_credit                           ,2)         avg_amount9_credit                               
,round(max_amount9_credit                           ,2)         max_amount9_credit                               
,round(min_amount9_credit                           ,2)         min_amount9_credit                               
,round(sum_cnt9_credit                              ,2)         sum_cnt9_credit                                  
,round(sum_amount43_creditcardrepayments            ,2)         sum_amount43_creditcardrepayments                
,round(avg_amount43_creditcardrepayments            ,2)         avg_amount43_creditcardrepayments                
,round(max_amount43_creditcardrepayments            ,2)         max_amount43_creditcardrepayments                
,round(min_amount43_creditcardrepayments            ,2)         min_amount43_creditcardrepayments                
,round(sum_cnt43_creditcardrepayments               ,2)         sum_cnt43_creditcardrepayments                   
,round(sum_amount32_enquiryfee                      ,2)         sum_amount32_enquiryfee                          
,round(avg_amount32_enquiryfee                      ,2)         avg_amount32_enquiryfee                          
,round(max_amount32_enquiryfee                      ,2)         max_amount32_enquiryfee                          
,round(sum_cnt32_enquiryfee                         ,2)         sum_cnt32_enquiryfee                             
,round(min_amount32_enquiryfee                      ,2)         min_amount32_enquiryfee                          
,round(sum_amount44_creditcardrepayments            ,2)         sum_amount44_creditcardrepayments                
,round(avg_amount44_creditcardrepayments            ,2)         avg_amount44_creditcardrepayments                
,round(max_amount44_creditcardrepayments            ,2)         max_amount44_creditcardrepayments                
,round(min_amount44_creditcardrepayments            ,2)         min_amount44_creditcardrepayments                
,round(sum_cnt44_creditcardrepayments               ,2)         sum_cnt44_creditcardrepayments                   
,round(sum_amount33_enquiryfee                      ,2)         sum_amount33_enquiryfee                          
,round(avg_amount33_enquiryfee                      ,2)         avg_amount33_enquiryfee                          
,round(max_amount33_enquiryfee                      ,2)         max_amount33_enquiryfee                          
,round(sum_cnt33_enquiryfee                         ,2)         sum_cnt33_enquiryfee                             
,round(min_amount33_enquiryfee                      ,2)         min_amount33_enquiryfee                          
 
from phone_variable_tnh_creditcardrepayments_20170724 
where 
 ( sum_amount1_credit                 <>0 
or avg_amount1_credit               <>0 
or max_amount1_credit               <>0 
or min_amount1_credit               <>0 
or sum_cnt1_credit                  <>0 
or sum_amount1_enquirybalance       <>0 
or sum_amount2_credit               <>0 
or avg_amount2_credit               <>0 
or max_amount2_credit               <>0 
or min_amount2_credit               <>0 
or sum_cnt2_credit                  <>0 
or sum_amount3_credit               <>0 
or avg_amount3_credit               <>0 
or max_amount3_credit               <>0 
or min_amount3_credit               <>0 
or sum_cnt3_credit                  <>0 
or sum_amount1_creditcardrepayments <>0 
or avg_amount1_creditcardrepayments <>0 
or max_amount1_creditcardrepayments <>0 
or min_amount1_creditcardrepayments <>0 
or sum_cnt1_creditcardrepayments    <>0 
or sum_amount1_enquiryfee           <>0 
or avg_amount1_enquiryfee           <>0 
or max_amount1_enquiryfee           <>0 
or min_amount1_enquiryfee           <>0 
or sum_cnt1_enquiryfee              <>0 
or sum_amount5_creditcardrepayments <>0 
or avg_amount5_creditcardrepayments <>0 
or max_amount5_creditcardrepayments <>0 
or min_amount5_creditcardrepayments <>0 
or sum_cnt5_creditcardrepayments    <>0 
or sum_amount2_creditcardrepayments <>0 
or avg_amount2_creditcardrepayments <>0 
or max_amount2_creditcardrepayments <>0 
or min_amount2_creditcardrepayments <>0 
or sum_cnt2_creditcardrepayments    <>0 
or sum_amount4_creditcardrepayments <>0 
or avg_amount4_creditcardrepayments <>0 
or max_amount4_creditcardrepayments <>0 
or min_amount4_creditcardrepayments <>0 
or sum_cnt4_creditcardrepayments    <>0 
or sum_amount6_creditcardrepayments <>0 
or avg_amount6_creditcardrepayments <>0 
or max_amount6_creditcardrepayments <>0 
or min_amount6_creditcardrepayments <>0 
or sum_cnt6_creditcardrepayments    <>0 
or sum_amount7_creditcardrepayments <>0 
or avg_amount7_creditcardrepayments <>0 
or max_amount7_creditcardrepayments <>0 
or min_amount7_creditcardrepayments <>0 
or sum_cnt7_creditcardrepayments    <>0 
or sum_amount9_creditcardrepayments <>0 
or avg_amount9_creditcardrepayments <>0 
or max_amount9_creditcardrepayments <>0 
or min_amount9_creditcardrepayments <>0 
or sum_cnt9_creditcardrepayments    <>0 
or sum_amount3_creditcardrepayments <>0 
or avg_amount3_creditcardrepayments <>0 
or max_amount3_creditcardrepayments <>0 
or min_amount3_creditcardrepayments <>0 
or sum_cnt3_creditcardrepayments    <>0 
or sum_amount8_creditcardrepayments <>0 
or avg_amount8_creditcardrepayments <>0 
or max_amount8_creditcardrepayments <>0 
or min_amount8_creditcardrepayments <>0 
or sum_cnt8_creditcardrepayments    <>0 
or sum_amount10_creditcardrepayments<>0 
or avg_amount10_creditcardrepayments<>0 
or max_amount10_creditcardrepayments<>0 
or min_amount10_creditcardrepayments<>0 
or sum_cnt10_creditcardrepayments   <>0 
or sum_amount11_creditcardrepayments<>0 
or avg_amount11_creditcardrepayments<>0 
or max_amount11_creditcardrepayments<>0 
or min_amount11_creditcardrepayments<>0 
or sum_cnt11_creditcardrepayments   <>0 
or sum_amount12_creditcardrepayments<>0 
or avg_amount12_creditcardrepayments<>0 
or max_amount12_creditcardrepayments<>0 
or min_amount12_creditcardrepayments<>0 
or sum_cnt12_creditcardrepayments   <>0 
or sum_amount2_enquiryfee           <>0 
or avg_amount2_enquiryfee           <>0 
or max_amount2_enquiryfee           <>0 
or min_amount2_enquiryfee           <>0 
or sum_cnt2_enquiryfee              <>0 
or sum_amount14_creditcardrepayments<>0 
or avg_amount14_creditcardrepayments<>0 
or max_amount14_creditcardrepayments<>0 
or min_amount14_creditcardrepayments<>0 
or sum_cnt14_creditcardrepayments   <>0 
or sum_amount13_creditcardrepayments<>0 
or avg_amount13_creditcardrepayments<>0 
or max_amount13_creditcardrepayments<>0 
or min_amount13_creditcardrepayments<>0 
or sum_cnt13_creditcardrepayments   <>0 
or sum_amount15_creditcardrepayments<>0 
or avg_amount15_creditcardrepayments<>0 
or max_amount15_creditcardrepayments<>0 
or min_amount15_creditcardrepayments<>0 
or sum_cnt15_creditcardrepayments   <>0 
or sum_amount16_creditcardrepayments<>0 
or avg_amount16_creditcardrepayments<>0 
or max_amount16_creditcardrepayments<>0 
or min_amount16_creditcardrepayments<>0 
or sum_cnt16_creditcardrepayments    <>0 
or sum_amount17_creditcardrepayments<>0 
or avg_amount17_creditcardrepayments<>0 
or max_amount17_creditcardrepayments<>0 
or min_amount17_creditcardrepayments<>0 
or sum_cnt17_creditcardrepayments   <>0 
or sum_amount3_enquiryfee           <>0 
or avg_amount3_enquiryfee           <>0 
or max_amount3_enquiryfee           <>0 
or min_amount3_enquiryfee           <>0 
or sum_cnt3_enquiryfee              <>0 
or sum_amount18_creditcardrepayments<>0 
or avg_amount18_creditcardrepayments<>0 
or max_amount18_creditcardrepayments<>0 
or min_amount18_creditcardrepayments<>0 
or sum_cnt18_creditcardrepayments   <>0 
or sum_amount4_credit               <>0 
or avg_amount4_credit               <>0 
or max_amount4_credit               <>0 
or min_amount4_credit               <>0 
or sum_cnt4_credit                  <>0 
or sum_amount19_creditcardrepayments<>0 
or avg_amount19_creditcardrepayments<>0 
or max_amount19_creditcardrepayments<>0 
or min_amount19_creditcardrepayments<>0 
or sum_cnt19_creditcardrepayments   <>0 
or sum_amount4_enquiryfee           <>0 
or avg_amount4_enquiryfee           <>0 
or max_amount4_enquiryfee           <>0 
or min_amount4_enquiryfee           <>0 
or sum_cnt4_enquiryfee              <>0 
or sum_amount20_creditcardrepayments<>0 
or avg_amount20_creditcardrepayments<>0 
or max_amount20_creditcardrepayments<>0 
or min_amount20_creditcardrepayments<>0 
or sum_cnt20_creditcardrepayments   <>0 
or sum_amount5_enquiryfee           <>0 
or avg_amount5_enquiryfee           <>0 
or max_amount5_enquiryfee           <>0 
or min_amount5_enquiryfee           <>0 
or sum_cnt5_enquiryfee              <>0 
or sum_amount6_enquiryfee           <>0 
or avg_amount6_enquiryfee           <>0 
or max_amount6_enquiryfee           <>0 
or min_amount6_enquiryfee           <>0 
or sum_cnt6_enquiryfee              <>0 
or sum_amount21_creditcardrepayments<>0 
or avg_amount21_creditcardrepayments<>0 
or max_amount21_creditcardrepayments<>0 
or min_amount21_creditcardrepayments<>0 
or sum_cnt21_creditcardrepayments   <>0 
or sum_amount7_enquiryfee           <>0 
or avg_amount7_enquiryfee           <>0 
or max_amount7_enquiryfee           <>0 
or sum_cnt7_enquiryfee              <>0 
or min_amount7_enquiryfee           <>0 
or sum_amount22_creditcardrepayments<>0 
or avg_amount22_creditcardrepayments<>0 
or max_amount22_creditcardrepayments<>0 
or min_amount22_creditcardrepayments<>0 
or sum_cnt22_creditcardrepayments   <>0 
or sum_amount8_enquiryfee           <>0 
or avg_amount8_enquiryfee           <>0 
or max_amount8_enquiryfee           <>0 
or sum_cnt8_enquiryfee              <>0 
or min_amount8_enquiryfee           <>0 
or sum_amount23_creditcardrepayments<>0 
or avg_amount23_creditcardrepayments<>0 
or max_amount23_creditcardrepayments<>0 
or min_amount23_creditcardrepayments<>0 
or sum_cnt23_creditcardrepayments   <>0 
or sum_amount9_enquiryfee           <>0 
or avg_amount9_enquiryfee           <>0 
or max_amount9_enquiryfee           <>0 
or min_amount9_enquiryfee           <>0 
or sum_cnt9_enquiryfee              <>0 
or sum_amount24_creditcardrepayments<>0 
or avg_amount24_creditcardrepayments<>0 
or max_amount24_creditcardrepayments<>0 
or min_amount24_creditcardrepayments<>0 
or sum_cnt24_creditcardrepayments   <>0 
or sum_amount10_enquiryfee          <>0 
or avg_amount10_enquiryfee          <>0 
or max_amount10_enquiryfee          <>0 
or min_amount10_enquiryfee          <>0 
or sum_cnt10_enquiryfee             <>0 
or sum_amount25_creditcardrepayments<>0 
or avg_amount25_creditcardrepayments<>0 
or max_amount25_creditcardrepayments<>0 
or min_amount25_creditcardrepayments<>0 
or sum_cnt25_creditcardrepayments   <>0 
or sum_amount11_enquiryfee          <>0 
or avg_amount11_enquiryfee          <>0 
or max_amount11_enquiryfee          <>0 
or min_amount11_enquiryfee          <>0 
or sum_cnt11_enquiryfee             <>0 
or sum_amount26_creditcardrepayments<>0 
or avg_amount26_creditcardrepayments<>0 
or max_amount26_creditcardrepayments<>0 
or min_amount26_creditcardrepayments<>0 
or sum_cnt26_creditcardrepayments   <>0 
or sum_amount12_enquiryfee          <>0 
or avg_amount12_enquiryfee          <>0 
or max_amount12_enquiryfee          <>0 
or min_amount12_enquiryfee          <>0 
or sum_cnt12_enquiryfee              <>0 
or sum_amount27_creditcardrepayments<>0 
or avg_amount27_creditcardrepayments<>0 
or max_amount27_creditcardrepayments<>0 
or min_amount27_creditcardrepayments<>0 
or sum_cnt27_creditcardrepayments   <>0 
or sum_amount13_enquiryfee          <>0 
or avg_amount13_enquiryfee          <>0 
or max_amount13_enquiryfee          <>0 
or min_amount13_enquiryfee          <>0 
or sum_cnt13_enquiryfee             <>0 
or sum_amount14_enquiryfee          <>0 
or avg_amount14_enquiryfee          <>0 
or max_amount14_enquiryfee          <>0 
or min_amount14_enquiryfee          <>0 
or sum_cnt14_enquiryfee             <>0 
or sum_amount28_creditcardrepayments<>0 
or avg_amount28_creditcardrepayments<>0 
or max_amount28_creditcardrepayments<>0 
or min_amount28_creditcardrepayments<>0 
or sum_cnt28_creditcardrepayments   <>0 
or sum_amount29_creditcardrepayments<>0 
or avg_amount29_creditcardrepayments<>0 
or max_amount29_creditcardrepayments<>0 
or min_amount29_creditcardrepayments<>0 
or sum_cnt29_creditcardrepayments   <>0 
or sum_amount15_enquiryfee          <>0 
or avg_amount15_enquiryfee          <>0 
or max_amount15_enquiryfee          <>0 
or sum_cnt15_enquiryfee             <>0 
or min_amount15_enquiryfee          <>0 
or sum_amount16_enquiryfee          <>0 
or avg_amount16_enquiryfee          <>0 
or max_amount16_enquiryfee          <>0 
or min_amount16_enquiryfee          <>0 
or sum_cnt16_enquiryfee             <>0 
or sum_amount30_creditcardrepayments<>0 
or avg_amount30_creditcardrepayments<>0 
or max_amount30_creditcardrepayments<>0 
or min_amount30_creditcardrepayments<>0 
or sum_cnt30_creditcardrepayments   <>0 
or sum_amount17_enquiryfee          <>0 
or avg_amount17_enquiryfee          <>0 
or max_amount17_enquiryfee          <>0 
or min_amount17_enquiryfee          <>0 
or sum_cnt17_enquiryfee             <>0 
or sum_amount5_credit               <>0 
or avg_amount5_credit               <>0 
or max_amount5_credit                 <>0 
or min_amount5_credit               <>0 
or sum_cnt5_credit                  <>0 
or sum_amount31_creditcardrepayments<>0 
or avg_amount31_creditcardrepayments<>0 
or max_amount31_creditcardrepayments<>0 
or min_amount31_creditcardrepayments<>0 
or sum_cnt31_creditcardrepayments   <>0 
or sum_amount18_enquiryfee          <>0 
or avg_amount18_enquiryfee          <>0 
or max_amount18_enquiryfee          <>0 
or sum_cnt18_enquiryfee             <>0 
or min_amount18_enquiryfee          <>0 
or sum_amount19_enquiryfee          <>0 
or avg_amount19_enquiryfee          <>0 
or max_amount19_enquiryfee          <>0 
or min_amount19_enquiryfee          <>0 
or sum_cnt19_enquiryfee             <>0 
or sum_amount32_creditcardrepayments<>0 
or avg_amount32_creditcardrepayments<>0 
or max_amount32_creditcardrepayments<>0 
or min_amount32_creditcardrepayments<>0 
or sum_cnt32_creditcardrepayments   <>0 
or sum_amount20_enquiryfee          <>0 
or avg_amount20_enquiryfee          <>0 
or max_amount20_enquiryfee          <>0 
or min_amount20_enquiryfee          <>0 
or sum_cnt20_enquiryfee             <>0 
or sum_amount33_creditcardrepayments<>0 
or avg_amount33_creditcardrepayments<>0 
or max_amount33_creditcardrepayments<>0 
or min_amount33_creditcardrepayments<>0 
or sum_cnt33_creditcardrepayments   <>0 
or sum_amount21_enquiryfee          <>0 
or avg_amount21_enquiryfee          <>0 
or max_amount21_enquiryfee          <>0 
or sum_cnt21_enquiryfee             <>0 
or min_amount21_enquiryfee          <>0 
or sum_amount34_creditcardrepayments<>0 
or avg_amount34_creditcardrepayments<>0 
or max_amount34_creditcardrepayments<>0 
or min_amount34_creditcardrepayments<>0 
or sum_cnt34_creditcardrepayments   <>0 
or sum_amount22_enquiryfee          <>0 
or avg_amount22_enquiryfee          <>0 
or max_amount22_enquiryfee          <>0 
or min_amount22_enquiryfee          <>0 
or sum_cnt22_enquiryfee             <>0 
or sum_amount35_creditcardrepayments<>0 
or avg_amount35_creditcardrepayments<>0 
or max_amount35_creditcardrepayments<>0 
or min_amount35_creditcardrepayments<>0 
or sum_cnt35_creditcardrepayments   <>0 
or sum_amount23_enquiryfee          <>0 
or avg_amount23_enquiryfee          <>0 
or max_amount23_enquiryfee          <>0 
or min_amount23_enquiryfee          <>0
or sum_cnt23_enquiryfee             <> 0 
or sum_amount6_credit               <> 0 
or avg_amount6_credit               <> 0 
or max_amount6_credit               <> 0 
or min_amount6_credit               <> 0 
or sum_cnt6_credit                  <> 0 
or sum_amount36_creditcardrepayments<> 0 
or avg_amount36_creditcardrepayments<> 0 
or max_amount36_creditcardrepayments<> 0 
or min_amount36_creditcardrepayments<> 0 
or sum_cnt36_creditcardrepayments   <> 0 
or sum_amount24_enquiryfee          <> 0 
or avg_amount24_enquiryfee          <> 0 
or max_amount24_enquiryfee          <> 0 
or min_amount24_enquiryfee          <> 0 
or sum_cnt24_enquiryfee             <> 0 
or sum_amount25_enquiryfee          <> 0 
or avg_amount25_enquiryfee          <> 0 
or max_amount25_enquiryfee          <> 0 
or sum_cnt25_enquiryfee             <> 0 
or min_amount25_enquiryfee          <> 0 
or sum_amount37_creditcardrepayments<> 0 
or avg_amount37_creditcardrepayments<> 0 
or max_amount37_creditcardrepayments<> 0 
or min_amount37_creditcardrepayments<> 0 
or sum_cnt37_creditcardrepayments   <> 0 
or sum_amount26_enquiryfee          <> 0 
or avg_amount26_enquiryfee          <> 0 
or max_amount26_enquiryfee          <> 0 
or min_amount26_enquiryfee          <> 0 
or sum_cnt26_enquiryfee             <> 0 
or sum_amount38_creditcardrepayments<> 0 
or avg_amount38_creditcardrepayments<> 0 
or max_amount38_creditcardrepayments<> 0 
or min_amount38_creditcardrepayments<> 0 
or sum_cnt38_creditcardrepayments   <> 0 
or sum_amount7_credit               <> 0 
or avg_amount7_credit               <> 0 
or max_amount7_credit               <> 0 
or min_amount7_credit               <> 0 
or sum_cnt7_credit                  <> 0 
or sum_amount27_enquiryfee          <> 0 
or avg_amount27_enquiryfee          <> 0 
or max_amount27_enquiryfee          <> 0 
or min_amount27_enquiryfee          <> 0 
or sum_cnt27_enquiryfee             <> 0 
or sum_amount39_creditcardrepayments<> 0 
or avg_amount39_creditcardrepayments<> 0 
or max_amount39_creditcardrepayments<> 0 
or min_amount39_creditcardrepayments<> 0 
or sum_cnt39_creditcardrepayments   <> 0 
or sum_amount28_enquiryfee          <> 0 
or avg_amount28_enquiryfee          <> 0 
or max_amount28_enquiryfee          <> 0 
or min_amount28_enquiryfee          <> 0 
or sum_cnt28_enquiryfee             <> 0 
or sum_amount40_creditcardrepayments<> 0 
or avg_amount40_creditcardrepayments<> 0 
or max_amount40_creditcardrepayments<> 0 
or min_amount40_creditcardrepayments<> 0 
or sum_cnt40_creditcardrepayments   <> 0 
or sum_amount29_enquiryfee          <> 0 
or avg_amount29_enquiryfee          <> 0 
or max_amount29_enquiryfee          <> 0 
or min_amount29_enquiryfee          <> 0 
or sum_cnt29_enquiryfee             <> 0 
or sum_amount8_credit               <> 0 
or avg_amount8_credit               <> 0 
or max_amount8_credit               <> 0 
or min_amount8_credit               <> 0 
or sum_cnt8_credit                  <> 0 
or sum_amount41_creditcardrepayments<> 0 
or avg_amount41_creditcardrepayments<> 0 
or max_amount41_creditcardrepayments<> 0 
or min_amount41_creditcardrepayments<> 0 
or sum_cnt41_creditcardrepayments   <> 0 
or sum_amount30_enquiryfee          <> 0 
or avg_amount30_enquiryfee          <> 0 
or max_amount30_enquiryfee          <> 0 
or min_amount30_enquiryfee          <> 0 
or sum_cnt30_enquiryfee             <> 0 
or sum_amount42_creditcardrepayments<> 0 
or avg_amount42_creditcardrepayments<> 0 
or max_amount42_creditcardrepayments<> 0 
or min_amount42_creditcardrepayments<> 0 
or sum_cnt42_creditcardrepayments   <> 0 
or sum_amount31_enquiryfee          <> 0 
or avg_amount31_enquiryfee          <> 0 
or max_amount31_enquiryfee          <> 0 
or min_amount31_enquiryfee          <> 0 
or sum_cnt31_enquiryfee             <> 0 
or sum_amount9_credit               <> 0 
or avg_amount9_credit               <> 0 
or max_amount9_credit               <> 0 
or min_amount9_credit               <> 0 
or sum_cnt9_credit                  <> 0 
or sum_amount43_creditcardrepayments<> 0 
or avg_amount43_creditcardrepayments<> 0 
or max_amount43_creditcardrepayments<> 0 
or min_amount43_creditcardrepayments<> 0 
or sum_cnt43_creditcardrepayments   <> 0 
or sum_amount32_enquiryfee          <> 0 
or avg_amount32_enquiryfee          <> 0 
or max_amount32_enquiryfee          <> 0 
or sum_cnt32_enquiryfee             <> 0 
or min_amount32_enquiryfee          <> 0 
or sum_amount44_creditcardrepayments<> 0 
or avg_amount44_creditcardrepayments<> 0
or max_amount44_creditcardrepayments<> 0
or min_amount44_creditcardrepayments<> 0
or sum_cnt44_creditcardrepayments   <> 0
or sum_amount33_enquiryfee          <> 0
or avg_amount33_enquiryfee          <> 0
or max_amount33_enquiryfee          <> 0
or sum_cnt33_enquiryfee             <> 0
or min_amount33_enquiryfee          <> 0

 ) 
;  
   
 
drop table   phone_variable_yfq_creditcardrepayments_20170724_result;
create table  phone_variable_yfq_creditcardrepayments_20170724_result_2 as
select 
   
phone
,round(sum_amount20_creditcardrepayments          ,2)    as       sum_amount20_creditcardrepayments           
,round(avg_amount20_creditcardrepayments          ,2)    as       avg_amount20_creditcardrepayments           
,round(max_amount20_creditcardrepayments          ,2)    as       max_amount20_creditcardrepayments           
,round(min_amount20_creditcardrepayments          ,2)    as       min_amount20_creditcardrepayments           
,round(sum_cnt20_creditcardrepayments             ,2)    as       sum_cnt20_creditcardrepayments              
,round(sum_amount19_creditcardrepayments          ,2)    as       sum_amount19_creditcardrepayments           
,round(avg_amount19_creditcardrepayments          ,2)    as       avg_amount19_creditcardrepayments           
,round(max_amount19_creditcardrepayments          ,2)    as       max_amount19_creditcardrepayments           
,round(min_amount19_creditcardrepayments          ,2)    as       min_amount19_creditcardrepayments           
,round(sum_cnt19_creditcardrepayments             ,2)    as       sum_cnt19_creditcardrepayments              
,round(sum_amount21_creditcardrepayments          ,2)    as       sum_amount21_creditcardrepayments           
,round(avg_amount21_creditcardrepayments          ,2)    as       avg_amount21_creditcardrepayments           
,round(max_amount21_creditcardrepayments          ,2)    as       max_amount21_creditcardrepayments           
,round(min_amount21_creditcardrepayments          ,2)    as       min_amount21_creditcardrepayments           
,round(sum_cnt21_creditcardrepayments             ,2)    as       sum_cnt21_creditcardrepayments              
,round(sum_amount18_creditcardrepayments          ,2)    as       sum_amount18_creditcardrepayments           
,round(avg_amount18_creditcardrepayments          ,2)    as       avg_amount18_creditcardrepayments           
,round(max_amount18_creditcardrepayments          ,2)    as       max_amount18_creditcardrepayments           
,round(min_amount18_creditcardrepayments          ,2)    as       min_amount18_creditcardrepayments           
,round(sum_cnt18_creditcardrepayments             ,2)    as       sum_cnt18_creditcardrepayments              
,round(sum_amount22_creditcardrepayments          ,2)    as       sum_amount22_creditcardrepayments           
,round(avg_amount22_creditcardrepayments          ,2)    as       avg_amount22_creditcardrepayments           
,round(max_amount22_creditcardrepayments          ,2)    as       max_amount22_creditcardrepayments           
,round(min_amount22_creditcardrepayments          ,2)    as       min_amount22_creditcardrepayments           
,round(sum_cnt22_creditcardrepayments             ,2)    as       sum_cnt22_creditcardrepayments              
,round(sum_amount17_creditcardrepayments          ,2)    as       sum_amount17_creditcardrepayments           
,round(avg_amount17_creditcardrepayments          ,2)    as       avg_amount17_creditcardrepayments           
,round(max_amount17_creditcardrepayments          ,2)    as       max_amount17_creditcardrepayments           
,round(min_amount17_creditcardrepayments          ,2)    as       min_amount17_creditcardrepayments           
,round(sum_cnt17_creditcardrepayments             ,2)    as       sum_cnt17_creditcardrepayments              
,round(sum_amount23_creditcardrepayments          ,2)    as       sum_amount23_creditcardrepayments           
,round(avg_amount23_creditcardrepayments          ,2)    as       avg_amount23_creditcardrepayments           
,round(max_amount23_creditcardrepayments          ,2)    as       max_amount23_creditcardrepayments           
,round(min_amount23_creditcardrepayments          ,2)    as       min_amount23_creditcardrepayments           
,round(sum_cnt23_creditcardrepayments             ,2)    as       sum_cnt23_creditcardrepayments              
,round(sum_amount16_creditcardrepayments          ,2)    as       sum_amount16_creditcardrepayments           
,round(avg_amount16_creditcardrepayments          ,2)    as       avg_amount16_creditcardrepayments           
,round(max_amount16_creditcardrepayments          ,2)    as       max_amount16_creditcardrepayments           
,round(min_amount16_creditcardrepayments          ,2)    as       min_amount16_creditcardrepayments           
,round(sum_cnt16_creditcardrepayments             ,2)    as       sum_cnt16_creditcardrepayments              
,round(sum_amount24_creditcardrepayments          ,2)    as       sum_amount24_creditcardrepayments           
,round(avg_amount24_creditcardrepayments          ,2)    as       avg_amount24_creditcardrepayments           
,round(max_amount24_creditcardrepayments          ,2)    as       max_amount24_creditcardrepayments           
,round(min_amount24_creditcardrepayments          ,2)    as       min_amount24_creditcardrepayments           
,round(sum_cnt24_creditcardrepayments             ,2)    as       sum_cnt24_creditcardrepayments              
,round(sum_amount15_creditcardrepayments          ,2)    as       sum_amount15_creditcardrepayments           
,round(avg_amount15_creditcardrepayments          ,2)    as       avg_amount15_creditcardrepayments           
,round(max_amount15_creditcardrepayments          ,2)    as       max_amount15_creditcardrepayments           
,round(min_amount15_creditcardrepayments          ,2)    as       min_amount15_creditcardrepayments           
,round(sum_cnt15_creditcardrepayments             ,2)    as       sum_cnt15_creditcardrepayments              
,round(sum_amount25_creditcardrepayments          ,2)    as       sum_amount25_creditcardrepayments           
,round(avg_amount25_creditcardrepayments          ,2)    as       avg_amount25_creditcardrepayments           
,round(max_amount25_creditcardrepayments          ,2)    as       max_amount25_creditcardrepayments           
,round(min_amount25_creditcardrepayments          ,2)    as       min_amount25_creditcardrepayments           
,round(sum_cnt25_creditcardrepayments             ,2)    as       sum_cnt25_creditcardrepayments              
,round(sum_amount14_creditcardrepayments          ,2)    as       sum_amount14_creditcardrepayments           
,round(avg_amount14_creditcardrepayments          ,2)    as       avg_amount14_creditcardrepayments           
,round(max_amount14_creditcardrepayments          ,2)    as       max_amount14_creditcardrepayments           
,round(min_amount14_creditcardrepayments          ,2)    as       min_amount14_creditcardrepayments           
,round(sum_cnt14_creditcardrepayments             ,2)    as       sum_cnt14_creditcardrepayments              
,round(sum_amount26_creditcardrepayments          ,2)    as       sum_amount26_creditcardrepayments           
,round(avg_amount26_creditcardrepayments          ,2)    as       avg_amount26_creditcardrepayments           
,round(max_amount26_creditcardrepayments          ,2)    as       max_amount26_creditcardrepayments           
,round(min_amount26_creditcardrepayments          ,2)    as       min_amount26_creditcardrepayments           
,round(sum_cnt26_creditcardrepayments             ,2)    as       sum_cnt26_creditcardrepayments              
,round(sum_amount13_creditcardrepayments          ,2)    as       sum_amount13_creditcardrepayments           
,round(avg_amount13_creditcardrepayments          ,2)    as       avg_amount13_creditcardrepayments           
,round(max_amount13_creditcardrepayments          ,2)    as       max_amount13_creditcardrepayments           
,round(min_amount13_creditcardrepayments          ,2)    as       min_amount13_creditcardrepayments           
,round(sum_cnt13_creditcardrepayments             ,2)    as       sum_cnt13_creditcardrepayments              
,round(sum_amount27_creditcardrepayments          ,2)    as       sum_amount27_creditcardrepayments           
,round(avg_amount27_creditcardrepayments          ,2)    as       avg_amount27_creditcardrepayments           
,round(max_amount27_creditcardrepayments          ,2)    as       max_amount27_creditcardrepayments           
,round(min_amount27_creditcardrepayments          ,2)    as       min_amount27_creditcardrepayments           
,round(sum_cnt27_creditcardrepayments             ,2)    as       sum_cnt27_creditcardrepayments              
,round(sum_amount12_creditcardrepayments          ,2)    as       sum_amount12_creditcardrepayments           
,round(avg_amount12_creditcardrepayments          ,2)    as       avg_amount12_creditcardrepayments           
,round(max_amount12_creditcardrepayments          ,2)    as       max_amount12_creditcardrepayments           
,round(min_amount12_creditcardrepayments          ,2)    as       min_amount12_creditcardrepayments           
,round(sum_cnt12_creditcardrepayments             ,2)    as       sum_cnt12_creditcardrepayments              
,round(sum_amount28_creditcardrepayments          ,2)    as       sum_amount28_creditcardrepayments           
,round(avg_amount28_creditcardrepayments          ,2)    as       avg_amount28_creditcardrepayments           
,round(max_amount28_creditcardrepayments          ,2)    as       max_amount28_creditcardrepayments           
,round(min_amount28_creditcardrepayments          ,2)    as       min_amount28_creditcardrepayments           
,round(sum_cnt28_creditcardrepayments             ,2)    as       sum_cnt28_creditcardrepayments              
,round(sum_amount11_creditcardrepayments          ,2)    as       sum_amount11_creditcardrepayments           
,round(avg_amount11_creditcardrepayments          ,2)    as       avg_amount11_creditcardrepayments           
,round(max_amount11_creditcardrepayments          ,2)    as       max_amount11_creditcardrepayments           
,round(min_amount11_creditcardrepayments          ,2)    as       min_amount11_creditcardrepayments           
,round(sum_cnt11_creditcardrepayments             ,2)    as       sum_cnt11_creditcardrepayments              
,round(sum_amount29_creditcardrepayments          ,2)    as       sum_amount29_creditcardrepayments           
,round(avg_amount29_creditcardrepayments          ,2)    as       avg_amount29_creditcardrepayments           
,round(max_amount29_creditcardrepayments          ,2)    as       max_amount29_creditcardrepayments           
,round(min_amount29_creditcardrepayments          ,2)    as       min_amount29_creditcardrepayments           
,round(sum_cnt29_creditcardrepayments             ,2)    as       sum_cnt29_creditcardrepayments              
,round(sum_amount10_creditcardrepayments          ,2)    as       sum_amount10_creditcardrepayments           
,round(avg_amount10_creditcardrepayments          ,2)    as       avg_amount10_creditcardrepayments           
,round(max_amount10_creditcardrepayments          ,2)    as       max_amount10_creditcardrepayments           
,round(min_amount10_creditcardrepayments          ,2)    as       min_amount10_creditcardrepayments           
,round(sum_cnt10_creditcardrepayments             ,2)    as       sum_cnt10_creditcardrepayments              
,round(sum_amount30_creditcardrepayments          ,2)    as       sum_amount30_creditcardrepayments           
,round(avg_amount30_creditcardrepayments          ,2)    as       avg_amount30_creditcardrepayments           
,round(max_amount30_creditcardrepayments          ,2)    as       max_amount30_creditcardrepayments           
,round(min_amount30_creditcardrepayments          ,2)    as       min_amount30_creditcardrepayments           
,round(sum_cnt30_creditcardrepayments             ,2)    as       sum_cnt30_creditcardrepayments              
,round(sum_amount9_creditcardrepayments           ,2)    as       sum_amount9_creditcardrepayments            
,round(avg_amount9_creditcardrepayments           ,2)    as       avg_amount9_creditcardrepayments            
,round(max_amount9_creditcardrepayments           ,2)    as       max_amount9_creditcardrepayments            
,round(min_amount9_creditcardrepayments           ,2)    as       min_amount9_creditcardrepayments            
,round(sum_cnt9_creditcardrepayments              ,2)    as       sum_cnt9_creditcardrepayments               
,round(sum_amount31_creditcardrepayments          ,2)    as       sum_amount31_creditcardrepayments           
,round(avg_amount31_creditcardrepayments          ,2)    as       avg_amount31_creditcardrepayments           
,round(max_amount31_creditcardrepayments          ,2)    as       max_amount31_creditcardrepayments           
,round(min_amount31_creditcardrepayments          ,2)    as       min_amount31_creditcardrepayments           
,round(sum_cnt31_creditcardrepayments             ,2)    as       sum_cnt31_creditcardrepayments              
,round(sum_amount8_creditcardrepayments           ,2)    as       sum_amount8_creditcardrepayments            
,round(avg_amount8_creditcardrepayments           ,2)    as       avg_amount8_creditcardrepayments            
,round(max_amount8_creditcardrepayments           ,2)    as       max_amount8_creditcardrepayments            
,round(min_amount8_creditcardrepayments           ,2)    as       min_amount8_creditcardrepayments            
,round(sum_cnt8_creditcardrepayments              ,2)    as       sum_cnt8_creditcardrepayments               
,round(sum_amount32_creditcardrepayments          ,2)    as       sum_amount32_creditcardrepayments           
,round(avg_amount32_creditcardrepayments          ,2)    as       avg_amount32_creditcardrepayments           
,round(max_amount32_creditcardrepayments          ,2)    as       max_amount32_creditcardrepayments           
,round(min_amount32_creditcardrepayments          ,2)    as       min_amount32_creditcardrepayments           
,round(sum_cnt32_creditcardrepayments             ,2)    as       sum_cnt32_creditcardrepayments              
,round(sum_amount7_creditcardrepayments           ,2)    as       sum_amount7_creditcardrepayments            
,round(avg_amount7_creditcardrepayments           ,2)    as       avg_amount7_creditcardrepayments            
,round(max_amount7_creditcardrepayments           ,2)    as       max_amount7_creditcardrepayments            
,round(min_amount7_creditcardrepayments           ,2)    as       min_amount7_creditcardrepayments            
,round(sum_cnt7_creditcardrepayments              ,2)    as       sum_cnt7_creditcardrepayments               
,round(sum_amount33_creditcardrepayments          ,2)    as       sum_amount33_creditcardrepayments           
,round(avg_amount33_creditcardrepayments          ,2)    as       avg_amount33_creditcardrepayments           
,round(max_amount33_creditcardrepayments          ,2)    as       max_amount33_creditcardrepayments           
,round(min_amount33_creditcardrepayments          ,2)    as       min_amount33_creditcardrepayments           
,round(sum_cnt33_creditcardrepayments             ,2)    as       sum_cnt33_creditcardrepayments              
,round(sum_amount17_enquiryfee                    ,2)    as    sum_amount17_enquiryfee                        
,round(avg_amount17_enquiryfee                    ,2)    as       avg_amount17_enquiryfee                     
,round(max_amount17_enquiryfee                    ,2)    as       max_amount17_enquiryfee                     
,round(sum_cnt17_enquiryfee                       ,2)    as       sum_cnt17_enquiryfee                        
,round(min_amount17_enquiryfee                    ,2)    as       min_amount17_enquiryfee                     
,round(sum_amount6_creditcardrepayments           ,2)    as       sum_amount6_creditcardrepayments            
,round(avg_amount6_creditcardrepayments           ,2)    as       avg_amount6_creditcardrepayments            
,round(max_amount6_creditcardrepayments           ,2)    as       max_amount6_creditcardrepayments            
,round(min_amount6_creditcardrepayments           ,2)    as       min_amount6_creditcardrepayments            
,round(sum_cnt6_creditcardrepayments              ,2)    as       sum_cnt6_creditcardrepayments               
,round(sum_amount16_enquiryfee                    ,2)    as    sum_amount16_enquiryfee                        
,round(avg_amount16_enquiryfee                    ,2)    as       avg_amount16_enquiryfee                     
,round(max_amount16_enquiryfee                    ,2)    as       max_amount16_enquiryfee                     
,round(sum_cnt16_enquiryfee                       ,2)    as       sum_cnt16_enquiryfee                        
,round(min_amount16_enquiryfee                    ,2)    as       min_amount16_enquiryfee                     
,round(sum_amount18_enquiryfee                    ,2)    as       sum_amount18_enquiryfee                     
,round(avg_amount18_enquiryfee                    ,2)    as       avg_amount18_enquiryfee                     
,round(max_amount18_enquiryfee                    ,2)    as       max_amount18_enquiryfee                     
,round(sum_cnt18_enquiryfee                       ,2)    as       sum_cnt18_enquiryfee                        
,round(min_amount18_enquiryfee                    ,2)    as       min_amount18_enquiryfee                     
,round(sum_amount34_creditcardrepayments          ,2)    as       sum_amount34_creditcardrepayments           
,round(avg_amount34_creditcardrepayments          ,2)    as       avg_amount34_creditcardrepayments           
,round(max_amount34_creditcardrepayments          ,2)    as       max_amount34_creditcardrepayments           
,round(min_amount34_creditcardrepayments          ,2)    as       min_amount34_creditcardrepayments           
,round(sum_cnt34_creditcardrepayments             ,2)    as       sum_cnt34_creditcardrepayments              
,round(sum_amount15_enquiryfee                    ,2)    as       sum_amount15_enquiryfee                     
,round(avg_amount15_enquiryfee                    ,2)    as       avg_amount15_enquiryfee                     
,round(max_amount15_enquiryfee                    ,2)    as       max_amount15_enquiryfee                     
,round(sum_cnt15_enquiryfee                       ,2)    as       sum_cnt15_enquiryfee                        
,round(min_amount15_enquiryfee                    ,2)    as       min_amount15_enquiryfee                     
,round(sum_amount19_enquiryfee                    ,2)    as       sum_amount19_enquiryfee                     
,round(avg_amount19_enquiryfee                    ,2)    as       avg_amount19_enquiryfee                     
,round(max_amount19_enquiryfee                    ,2)    as       max_amount19_enquiryfee                     
,round(sum_cnt19_enquiryfee                       ,2)    as       sum_cnt19_enquiryfee                        
,round(min_amount19_enquiryfee                    ,2)    as       min_amount19_enquiryfee                     
,round(sum_amount20_enquiryfee                    ,2)    as       sum_amount20_enquiryfee                     
,round(avg_amount20_enquiryfee                    ,2)    as       avg_amount20_enquiryfee                     
,round(max_amount20_enquiryfee                    ,2)    as       max_amount20_enquiryfee                     
,round(sum_cnt20_enquiryfee                       ,2)    as       sum_cnt20_enquiryfee                        
,round(min_amount20_enquiryfee                    ,2)    as       min_amount20_enquiryfee                     
,round(sum_amount14_enquiryfee                    ,2)    as       sum_amount14_enquiryfee                     
,round(avg_amount14_enquiryfee                    ,2)    as       avg_amount14_enquiryfee                     
,round(max_amount14_enquiryfee                    ,2)    as       max_amount14_enquiryfee                     
,round(sum_cnt14_enquiryfee                       ,2)    as       sum_cnt14_enquiryfee                        
,round(min_amount14_enquiryfee                    ,2)    as       min_amount14_enquiryfee                     
,round(sum_amount21_enquiryfee                    ,2)    as       sum_amount21_enquiryfee                     
,round(avg_amount21_enquiryfee                    ,2)    as       avg_amount21_enquiryfee                     
,round(max_amount21_enquiryfee                    ,2)    as       max_amount21_enquiryfee                     
,round(sum_cnt21_enquiryfee                       ,2)    as       sum_cnt21_enquiryfee                        
,round(min_amount21_enquiryfee                    ,2)    as       min_amount21_enquiryfee                     
,round(sum_amount13_enquiryfee                    ,2)    as       sum_amount13_enquiryfee                     
,round(avg_amount13_enquiryfee                    ,2)    as       avg_amount13_enquiryfee                     
,round(max_amount13_enquiryfee                    ,2)    as       max_amount13_enquiryfee                     
,round(sum_cnt13_enquiryfee                       ,2)    as       sum_cnt13_enquiryfee                        
,round(min_amount13_enquiryfee                    ,2)    as       min_amount13_enquiryfee                     
,round(sum_amount22_enquiryfee                    ,2)    as       sum_amount22_enquiryfee                     
,round(avg_amount22_enquiryfee                    ,2)    as       avg_amount22_enquiryfee                     
,round(max_amount22_enquiryfee                    ,2)    as       max_amount22_enquiryfee                     
,round(sum_cnt22_enquiryfee                       ,2)    as       sum_cnt22_enquiryfee                        
,round(min_amount22_enquiryfee                    ,2)    as       min_amount22_enquiryfee                     
,round(sum_amount12_enquiryfee                    ,2)    as       sum_amount12_enquiryfee                     
,round(avg_amount12_enquiryfee                    ,2)    as       avg_amount12_enquiryfee                     
,round(max_amount12_enquiryfee                    ,2)    as       max_amount12_enquiryfee                     
,round(min_amount12_enquiryfee                    ,2)    as       min_amount12_enquiryfee                     
,round(sum_cnt12_enquiryfee                       ,2)    as       sum_cnt12_enquiryfee                        
,round(sum_amount35_creditcardrepayments          ,2)    as       sum_amount35_creditcardrepayments           
,round(avg_amount35_creditcardrepayments          ,2)    as       avg_amount35_creditcardrepayments           
,round(max_amount35_creditcardrepayments          ,2)    as       max_amount35_creditcardrepayments           
,round(min_amount35_creditcardrepayments          ,2)    as       min_amount35_creditcardrepayments           
,round(sum_cnt35_creditcardrepayments             ,2)    as       sum_cnt35_creditcardrepayments              
,round(sum_amount5_creditcardrepayments           ,2)    as       sum_amount5_creditcardrepayments            
,round(avg_amount5_creditcardrepayments           ,2)    as       avg_amount5_creditcardrepayments            
,round(max_amount5_creditcardrepayments           ,2)    as       max_amount5_creditcardrepayments            
,round(min_amount5_creditcardrepayments           ,2)    as       min_amount5_creditcardrepayments            
,round(sum_cnt5_creditcardrepayments              ,2)    as       sum_cnt5_creditcardrepayments               
,round(sum_amount23_enquiryfee                    ,2)    as       sum_amount23_enquiryfee                     
,round(avg_amount23_enquiryfee                    ,2)    as       avg_amount23_enquiryfee                     
,round(max_amount23_enquiryfee                    ,2)    as       max_amount23_enquiryfee                     
,round(sum_cnt23_enquiryfee                       ,2)    as       sum_cnt23_enquiryfee                        
,round(min_amount23_enquiryfee                    ,2)    as       min_amount23_enquiryfee                     
,round(sum_amount11_enquiryfee                    ,2)    as       sum_amount11_enquiryfee                     
,round(avg_amount11_enquiryfee                    ,2)    as       avg_amount11_enquiryfee                     
,round(max_amount11_enquiryfee                    ,2)    as       max_amount11_enquiryfee                     
,round(min_amount11_enquiryfee                    ,2)    as       min_amount11_enquiryfee                     
,round(sum_cnt11_enquiryfee                       ,2)    as       sum_cnt11_enquiryfee                        
,round(sum_amount24_enquiryfee                    ,2)    as       sum_amount24_enquiryfee                     
,round(avg_amount24_enquiryfee                    ,2)    as       avg_amount24_enquiryfee                     
,round(max_amount24_enquiryfee                    ,2)    as       max_amount24_enquiryfee                     
,round(sum_cnt24_enquiryfee                       ,2)    as       sum_cnt24_enquiryfee                        
,round(min_amount24_enquiryfee                    ,2)    as       min_amount24_enquiryfee                     
,round(sum_amount10_enquiryfee                    ,2)    as       sum_amount10_enquiryfee                     
,round(avg_amount10_enquiryfee                    ,2)    as       avg_amount10_enquiryfee                     
,round(max_amount10_enquiryfee                    ,2)    as       max_amount10_enquiryfee                     
,round(sum_cnt10_enquiryfee                       ,2)    as       sum_cnt10_enquiryfee                        
,round(min_amount10_enquiryfee                    ,2)    as       min_amount10_enquiryfee                     
,round(sum_amount36_creditcardrepayments          ,2)    as       sum_amount36_creditcardrepayments           
,round(avg_amount36_creditcardrepayments          ,2)    as       avg_amount36_creditcardrepayments           
,round(max_amount36_creditcardrepayments          ,2)    as       max_amount36_creditcardrepayments           
,round(min_amount36_creditcardrepayments          ,2)    as       min_amount36_creditcardrepayments           
,round(sum_cnt36_creditcardrepayments             ,2)    as       sum_cnt36_creditcardrepayments              
,round(sum_amount4_creditcardrepayments           ,2)    as       sum_amount4_creditcardrepayments            
,round(avg_amount4_creditcardrepayments           ,2)    as       avg_amount4_creditcardrepayments            
,round(max_amount4_creditcardrepayments           ,2)    as       max_amount4_creditcardrepayments            
,round(min_amount4_creditcardrepayments           ,2)    as       min_amount4_creditcardrepayments            
,round(sum_cnt4_creditcardrepayments              ,2)    as       sum_cnt4_creditcardrepayments               
,round(sum_amount9_enquiryfee                     ,2)    as       sum_amount9_enquiryfee                      
,round(avg_amount9_enquiryfee                     ,2)    as       avg_amount9_enquiryfee                      
,round(max_amount9_enquiryfee                     ,2)    as       max_amount9_enquiryfee                      
,round(sum_cnt9_enquiryfee                        ,2)    as      sum_cnt9_enquiryfee                          
,round(min_amount9_enquiryfee                     ,2)    as       min_amount9_enquiryfee                      
,round(sum_amount25_enquiryfee                    ,2)    as       sum_amount25_enquiryfee                     
,round(avg_amount25_enquiryfee                    ,2)    as       avg_amount25_enquiryfee                     
,round(max_amount25_enquiryfee                    ,2)    as       max_amount25_enquiryfee                     
,round(sum_cnt25_enquiryfee                       ,2)    as       sum_cnt25_enquiryfee                        
,round(min_amount25_enquiryfee                    ,2)    as       min_amount25_enquiryfee                     
,round(sum_amount8_enquiryfee                     ,2)    as       sum_amount8_enquiryfee                      
,round(avg_amount8_enquiryfee                     ,2)    as       avg_amount8_enquiryfee                      
,round(max_amount8_enquiryfee                     ,2)    as       max_amount8_enquiryfee                      
,round(sum_cnt8_enquiryfee                        ,2)    as      sum_cnt8_enquiryfee                          
,round(min_amount8_enquiryfee                     ,2)    as       min_amount8_enquiryfee                      
,round(sum_amount37_creditcardrepayments          ,2)    as       sum_amount37_creditcardrepayments           
,round(avg_amount37_creditcardrepayments          ,2)    as       avg_amount37_creditcardrepayments           
,round(max_amount37_creditcardrepayments          ,2)    as       max_amount37_creditcardrepayments           
,round(min_amount37_creditcardrepayments          ,2)    as       min_amount37_creditcardrepayments           
,round(sum_cnt37_creditcardrepayments             ,2)    as       sum_cnt37_creditcardrepayments              
,round(sum_amount3_creditcardrepayments           ,2)    as       sum_amount3_creditcardrepayments            
,round(avg_amount3_creditcardrepayments           ,2)    as       avg_amount3_creditcardrepayments            
,round(max_amount3_creditcardrepayments           ,2)    as       max_amount3_creditcardrepayments            
,round(min_amount3_creditcardrepayments           ,2)    as       min_amount3_creditcardrepayments            
,round(sum_cnt3_creditcardrepayments              ,2)    as       sum_cnt3_creditcardrepayments               
,round(sum_amount26_enquiryfee                    ,2)    as       sum_amount26_enquiryfee                     
,round(avg_amount26_enquiryfee                    ,2)    as       avg_amount26_enquiryfee                     
,round(max_amount26_enquiryfee                    ,2)    as       max_amount26_enquiryfee                     
,round(sum_cnt26_enquiryfee                       ,2)    as       sum_cnt26_enquiryfee                        
,round(min_amount26_enquiryfee                    ,2)    as       min_amount26_enquiryfee                     
,round(sum_amount7_enquiryfee                     ,2)    as       sum_amount7_enquiryfee                      
,round(avg_amount7_enquiryfee                     ,2)    as       avg_amount7_enquiryfee                      
,round(max_amount7_enquiryfee                     ,2)    as       max_amount7_enquiryfee                      
,round(sum_cnt7_enquiryfee                        ,2)    as      sum_cnt7_enquiryfee                          
,round(min_amount7_enquiryfee                     ,2)    as       min_amount7_enquiryfee                      
,round(sum_amount27_enquiryfee                    ,2)    as       sum_amount27_enquiryfee                     
,round(avg_amount27_enquiryfee                    ,2)    as       avg_amount27_enquiryfee                     
,round(max_amount27_enquiryfee                    ,2)    as       max_amount27_enquiryfee                     
,round(sum_cnt27_enquiryfee                       ,2)    as       sum_cnt27_enquiryfee                        
,round(min_amount27_enquiryfee                    ,2)    as       min_amount27_enquiryfee                     
,round(sum_amount38_creditcardrepayments          ,2)    as       sum_amount38_creditcardrepayments           
,round(avg_amount38_creditcardrepayments          ,2)    as       avg_amount38_creditcardrepayments           
,round(max_amount38_creditcardrepayments          ,2)    as       max_amount38_creditcardrepayments           
,round(min_amount38_creditcardrepayments          ,2)    as       min_amount38_creditcardrepayments           
,round(sum_cnt38_creditcardrepayments             ,2)    as       sum_cnt38_creditcardrepayments              
,round(sum_amount6_enquiryfee                     ,2)    as       sum_amount6_enquiryfee                      
,round(avg_amount6_enquiryfee                     ,2)    as       avg_amount6_enquiryfee                      
,round(max_amount6_enquiryfee                     ,2)    as       max_amount6_enquiryfee                      
,round(sum_cnt6_enquiryfee                        ,2)    as      sum_cnt6_enquiryfee                          
,round(min_amount6_enquiryfee                     ,2)    as       min_amount6_enquiryfee                      
,round(sum_amount2_creditcardrepayments           ,2)    as       sum_amount2_creditcardrepayments            
,round(avg_amount2_creditcardrepayments           ,2)    as       avg_amount2_creditcardrepayments            
,round(max_amount2_creditcardrepayments           ,2)    as       max_amount2_creditcardrepayments            
,round(min_amount2_creditcardrepayments           ,2)    as       min_amount2_creditcardrepayments            
,round(sum_cnt2_creditcardrepayments              ,2)    as       sum_cnt2_creditcardrepayments               
,round(sum_amount1_creditcardrepayments           ,2)    as       sum_amount1_creditcardrepayments            
,round(avg_amount1_creditcardrepayments           ,2)    as       avg_amount1_creditcardrepayments            
,round(max_amount1_creditcardrepayments           ,2)    as       max_amount1_creditcardrepayments            
,round(min_amount1_creditcardrepayments           ,2)    as       min_amount1_creditcardrepayments            
,round(sum_cnt1_creditcardrepayments              ,2)    as       sum_cnt1_creditcardrepayments               
,round(sum_amount28_enquiryfee                    ,2)    as       sum_amount28_enquiryfee                     
,round(avg_amount28_enquiryfee                    ,2)    as       avg_amount28_enquiryfee                     
,round(max_amount28_enquiryfee                    ,2)    as       max_amount28_enquiryfee                     
,round(sum_cnt28_enquiryfee                       ,2)    as       sum_cnt28_enquiryfee                        
,round(min_amount28_enquiryfee                    ,2)    as       min_amount28_enquiryfee                     
,round(sum_amount5_enquiryfee                     ,2)    as       sum_amount5_enquiryfee                      
,round(avg_amount5_enquiryfee                     ,2)    as       avg_amount5_enquiryfee                      
,round(max_amount5_enquiryfee                     ,2)    as       max_amount5_enquiryfee                      
,round(sum_cnt5_enquiryfee                        ,2)    as      sum_cnt5_enquiryfee                          
,round(min_amount5_enquiryfee                     ,2)    as       min_amount5_enquiryfee                      
,round(sum_amount39_creditcardrepayments          ,2)    as       sum_amount39_creditcardrepayments           
,round(avg_amount39_creditcardrepayments          ,2)    as       avg_amount39_creditcardrepayments           
,round(max_amount39_creditcardrepayments          ,2)    as       max_amount39_creditcardrepayments           
,round(min_amount39_creditcardrepayments          ,2)    as       min_amount39_creditcardrepayments           
,round(sum_cnt39_creditcardrepayments             ,2)    as       sum_cnt39_creditcardrepayments              
,round(sum_amount4_enquiryfee                     ,2)    as       sum_amount4_enquiryfee                      
,round(avg_amount4_enquiryfee                     ,2)    as       avg_amount4_enquiryfee                      
,round(max_amount4_enquiryfee                     ,2)    as       max_amount4_enquiryfee                      
,round(sum_cnt4_enquiryfee                        ,2)    as      sum_cnt4_enquiryfee                          
,round(min_amount4_enquiryfee                     ,2)    as       min_amount4_enquiryfee                      
,round(sum_amount29_enquiryfee                    ,2)    as       sum_amount29_enquiryfee                     
,round(avg_amount29_enquiryfee                    ,2)    as       avg_amount29_enquiryfee                     
,round(max_amount29_enquiryfee                    ,2)    as       max_amount29_enquiryfee                     
,round(sum_cnt29_enquiryfee                       ,2)    as       sum_cnt29_enquiryfee                        
,round(min_amount29_enquiryfee                    ,2)    as       min_amount29_enquiryfee                     
,round(sum_amount3_enquiryfee                     ,2)    as       sum_amount3_enquiryfee                      
,round(avg_amount3_enquiryfee                     ,2)    as       avg_amount3_enquiryfee                      
,round(max_amount3_enquiryfee                     ,2)    as       max_amount3_enquiryfee                      
,round(min_amount3_enquiryfee                     ,2)    as       min_amount3_enquiryfee                      
,round(sum_cnt3_enquiryfee                        ,2)    as      sum_cnt3_enquiryfee                          
,round(sum_amount40_creditcardrepayments          ,2)    as       sum_amount40_creditcardrepayments           
,round(avg_amount40_creditcardrepayments          ,2)    as       avg_amount40_creditcardrepayments           
,round(max_amount40_creditcardrepayments          ,2)    as       max_amount40_creditcardrepayments           
,round(min_amount40_creditcardrepayments          ,2)    as       min_amount40_creditcardrepayments           
,round(sum_cnt40_creditcardrepayments             ,2)    as       sum_cnt40_creditcardrepayments              
,round(sum_amount1_credit                         ,2)    as       sum_amount1_credit                          
,round(avg_amount1_credit                         ,2)    as       avg_amount1_credit                          
,round(max_amount1_credit                         ,2)    as       max_amount1_credit                          
,round(min_amount1_credit                         ,2)    as       min_amount1_credit                          
,round(sum_cnt1_credit                            ,2)    as       sum_cnt1_credit                             
,round(sum_amount30_enquiryfee                    ,2)    as       sum_amount30_enquiryfee                     
,round(avg_amount30_enquiryfee                    ,2)    as       avg_amount30_enquiryfee                     
,round(max_amount30_enquiryfee                    ,2)    as       max_amount30_enquiryfee                     
,round(sum_cnt30_enquiryfee                       ,2)    as       sum_cnt30_enquiryfee                        
,round(min_amount30_enquiryfee                    ,2)    as       min_amount30_enquiryfee                     
,round(sum_amount2_enquiryfee                     ,2)    as       sum_amount2_enquiryfee                      
,round(avg_amount2_enquiryfee                     ,2)    as       avg_amount2_enquiryfee                      
,round(max_amount2_enquiryfee                     ,2)    as       max_amount2_enquiryfee                      
,round(sum_cnt2_enquiryfee                        ,2)    as      sum_cnt2_enquiryfee                          
,round(min_amount2_enquiryfee                     ,2)    as       min_amount2_enquiryfee                      
,round(sum_amount1_enquiryfee                     ,2)    as       sum_amount1_enquiryfee                      
,round(avg_amount1_enquiryfee                     ,2)    as       avg_amount1_enquiryfee                      
,round(max_amount1_enquiryfee                     ,2)    as       max_amount1_enquiryfee                      
,round(sum_cnt1_enquiryfee                        ,2)    as      sum_cnt1_enquiryfee                          
,round(min_amount1_enquiryfee                     ,2)    as       min_amount1_enquiryfee                      
,round(sum_amount41_creditcardrepayments          ,2)    as       sum_amount41_creditcardrepayments           
,round(avg_amount41_creditcardrepayments          ,2)    as       avg_amount41_creditcardrepayments           
,round(max_amount41_creditcardrepayments          ,2)    as       max_amount41_creditcardrepayments           
,round(min_amount41_creditcardrepayments          ,2)    as       min_amount41_creditcardrepayments           
,round(sum_cnt41_creditcardrepayments             ,2)    as       sum_cnt41_creditcardrepayments              
,round(sum_amount2_credit                         ,2)    as       sum_amount2_credit                          
,round(avg_amount2_credit                         ,2)    as       avg_amount2_credit                          
,round(max_amount2_credit                         ,2)    as       max_amount2_credit                          
,round(min_amount2_credit                         ,2)    as       min_amount2_credit                          
,round(sum_cnt2_credit                            ,2)    as       sum_cnt2_credit                             
,round(sum_amount31_enquiryfee                    ,2)    as       sum_amount31_enquiryfee                     
,round(avg_amount31_enquiryfee                    ,2)    as       avg_amount31_enquiryfee                     
,round(max_amount31_enquiryfee                    ,2)    as       max_amount31_enquiryfee                     
,round(sum_cnt31_enquiryfee                       ,2)    as       sum_cnt31_enquiryfee                        
,round(min_amount31_enquiryfee                    ,2)    as       min_amount31_enquiryfee                     
,round(sum_amount3_credit                         ,2)    as       sum_amount3_credit                          
,round(avg_amount3_credit                         ,2)    as       avg_amount3_credit                          
,round(max_amount3_credit                         ,2)    as       max_amount3_credit                          
,round(min_amount3_credit                         ,2)    as       min_amount3_credit                          
,round(sum_cnt3_credit                            ,2)    as       sum_cnt3_credit                             
,round(sum_amount42_creditcardrepayments          ,2)    as       sum_amount42_creditcardrepayments           
,round(avg_amount42_creditcardrepayments          ,2)    as       avg_amount42_creditcardrepayments           
,round(max_amount42_creditcardrepayments          ,2)    as       max_amount42_creditcardrepayments           
,round(min_amount42_creditcardrepayments          ,2)    as       min_amount42_creditcardrepayments           
,round(sum_cnt42_creditcardrepayments             ,2)    as       sum_cnt42_creditcardrepayments              
,round(sum_amount4_credit                         ,2)    as       sum_amount4_credit                          
,round(avg_amount4_credit                         ,2)    as       avg_amount4_credit                          
,round(max_amount4_credit                         ,2)    as       max_amount4_credit                          
,round(min_amount4_credit                         ,2)    as       min_amount4_credit                          
,round(sum_cnt4_credit                            ,2)    as       sum_cnt4_credit                             
,round(sum_amount32_enquiryfee                    ,2)    as       sum_amount32_enquiryfee                     
,round(avg_amount32_enquiryfee                    ,2)    as       avg_amount32_enquiryfee                     
,round(max_amount32_enquiryfee                    ,2)    as       max_amount32_enquiryfee                     
,round(sum_cnt32_enquiryfee                       ,2)    as       sum_cnt32_enquiryfee                        
,round(min_amount32_enquiryfee                    ,2)    as       min_amount32_enquiryfee                     
,round(sum_amount5_credit                         ,2)    as       sum_amount5_credit                          
,round(avg_amount5_credit                         ,2)    as       avg_amount5_credit                          
,round(max_amount5_credit                         ,2)    as       max_amount5_credit                          
,round(min_amount5_credit                         ,2)    as       min_amount5_credit                          
,round(sum_cnt5_credit                            ,2)    as       sum_cnt5_credit                             
,round(sum_amount43_creditcardrepayments          ,2)    as       sum_amount43_creditcardrepayments           
,round(avg_amount43_creditcardrepayments          ,2)    as       avg_amount43_creditcardrepayments           
,round(max_amount43_creditcardrepayments          ,2)    as       max_amount43_creditcardrepayments           
,round(min_amount43_creditcardrepayments          ,2)    as       min_amount43_creditcardrepayments           
,round(sum_cnt43_creditcardrepayments             ,2)    as       sum_cnt43_creditcardrepayments              
,round(sum_amount6_credit                         ,2)    as       sum_amount6_credit                          
,round(avg_amount6_credit                         ,2)    as       avg_amount6_credit                          
,round(max_amount6_credit                         ,2)    as       max_amount6_credit                          
,round(min_amount6_credit                         ,2)    as       min_amount6_credit                          
,round(sum_cnt6_credit                            ,2)    as       sum_cnt6_credit                             
,round(sum_amount33_enquiryfee                    ,2)    as       sum_amount33_enquiryfee                     
,round(avg_amount33_enquiryfee                    ,2)    as       avg_amount33_enquiryfee                     
,round(max_amount33_enquiryfee                    ,2)    as       max_amount33_enquiryfee                     
,round(sum_cnt33_enquiryfee                       ,2)    as       sum_cnt33_enquiryfee                        
,round(min_amount33_enquiryfee                    ,2)    as    min_amount33_enquiryfee                        
,round(sum_amount44_creditcardrepayments          ,2)    as       sum_amount44_creditcardrepayments           
,round(avg_amount44_creditcardrepayments          ,2)    as       avg_amount44_creditcardrepayments           
,round(max_amount44_creditcardrepayments          ,2)    as       max_amount44_creditcardrepayments           
,round(min_amount44_creditcardrepayments          ,2)    as       min_amount44_creditcardrepayments           
,round(sum_cnt44_creditcardrepayments             ,2)    as       sum_cnt44_creditcardrepayments              
,round(sum_amount34_enquiryfee                    ,2)    as       sum_amount34_enquiryfee                     
,round(avg_amount34_enquiryfee                    ,2)    as       avg_amount34_enquiryfee                     
,round(max_amount34_enquiryfee                    ,2)    as       max_amount34_enquiryfee                     
,round(sum_cnt34_enquiryfee                       ,2)    as       sum_cnt34_enquiryfee                        
,round(min_amount34_enquiryfee                    ,2)    as       min_amount34_enquiryfee                     
,round(sum_amount45_creditcardrepayments          ,2)    as    sum_amount45_creditcardrepayments              
,round(avg_amount45_creditcardrepayments          ,2)    as       avg_amount45_creditcardrepayments           
,round(max_amount45_creditcardrepayments          ,2)    as       max_amount45_creditcardrepayments           
,round(min_amount45_creditcardrepayments          ,2)    as       min_amount45_creditcardrepayments           
,round(sum_cnt45_creditcardrepayments             ,2)    as       sum_cnt45_creditcardrepayments              
,round(sum_amount7_credit                         ,2)    as       sum_amount7_credit                          
,round(avg_amount7_credit                         ,2)    as       avg_amount7_credit                          
,round(max_amount7_credit                         ,2)    as       max_amount7_credit                          
,round(min_amount7_credit                         ,2)    as       min_amount7_credit                          
,round(sum_cnt7_credit                            ,2)    as       sum_cnt7_credit                             
,round(sum_amount35_enquiryfee                    ,2)    as       sum_amount35_enquiryfee                     
,round(avg_amount35_enquiryfee                    ,2)    as       avg_amount35_enquiryfee                     
,round(max_amount35_enquiryfee                    ,2)    as       max_amount35_enquiryfee                     
,round(sum_cnt35_enquiryfee                       ,2)    as       sum_cnt35_enquiryfee                        
,round(min_amount35_enquiryfee                    ,2)    as       min_amount35_enquiryfee                     
,round(sum_amount46_creditcardrepayments          ,2)    as    sum_amount46_creditcardrepayments              
,round(avg_amount46_creditcardrepayments          ,2)    as       avg_amount46_creditcardrepayments           
,round(max_amount46_creditcardrepayments          ,2)    as       max_amount46_creditcardrepayments           
,round(min_amount46_creditcardrepayments          ,2)    as       min_amount46_creditcardrepayments           
,round(sum_cnt46_creditcardrepayments             ,2)    as       sum_cnt46_creditcardrepayments              
,round(sum_amount8_credit                         ,2)    as       sum_amount8_credit                          
,round(avg_amount8_credit                         ,2)    as       avg_amount8_credit                          
,round(max_amount8_credit                         ,2)    as       max_amount8_credit                          
,round(min_amount8_credit                         ,2)    as       min_amount8_credit                          
,round(sum_cnt8_credit                            ,2)    as       sum_cnt8_credit                             
,round(sum_amount36_enquiryfee                    ,2)    as       sum_amount36_enquiryfee                     
,round(avg_amount36_enquiryfee                    ,2)    as       avg_amount36_enquiryfee                     
,round(max_amount36_enquiryfee                    ,2)    as       max_amount36_enquiryfee                     
,round(sum_cnt36_enquiryfee                       ,2)    as       sum_cnt36_enquiryfee                        
,round(min_amount36_enquiryfee                    ,2)    as       min_amount36_enquiryfee                     
,round(sum_amount47_creditcardrepayments          ,2)    as       sum_amount47_creditcardrepayments           
,round(avg_amount47_creditcardrepayments          ,2)    as    avg_amount47_creditcardrepayments              
,round(max_amount47_creditcardrepayments          ,2)    as       max_amount47_creditcardrepayments           
,round(min_amount47_creditcardrepayments          ,2)    as       min_amount47_creditcardrepayments           
,round(sum_cnt47_creditcardrepayments             ,2)    as       sum_cnt47_creditcardrepayments              
,round(sum_amount9_credit                         ,2)    as       sum_amount9_credit                          
,round(avg_amount9_credit                         ,2)    as       avg_amount9_credit                          
,round(max_amount9_credit                         ,2)    as       max_amount9_credit                          
,round(min_amount9_credit                         ,2)    as       min_amount9_credit                          
,round(sum_cnt9_credit                            ,2)    as       sum_cnt9_credit                             
,round(sum_amount37_enquiryfee                    ,2)    as       sum_amount37_enquiryfee                     
,round(avg_amount37_enquiryfee                    ,2)    as       avg_amount37_enquiryfee                     
,round(max_amount37_enquiryfee                    ,2)    as       max_amount37_enquiryfee                     
,round(sum_cnt37_enquiryfee                       ,2)    as       sum_cnt37_enquiryfee                        
,round(min_amount37_enquiryfee                    ,2)    as       min_amount37_enquiryfee                     
,round(sum_amount48_creditcardrepayments          ,2)    as       sum_amount48_creditcardrepayments           
,round(avg_amount48_creditcardrepayments          ,2)    as       avg_amount48_creditcardrepayments           
,round(max_amount48_creditcardrepayments          ,2)    as    max_amount48_creditcardrepayments              
,round(min_amount48_creditcardrepayments          ,2)    as       min_amount48_creditcardrepayments           
,round(sum_cnt48_creditcardrepayments             ,2)    as       sum_cnt48_creditcardrepayments              
,round(sum_amount38_enquiryfee                    ,2)    as       sum_amount38_enquiryfee                     
,round(avg_amount38_enquiryfee                    ,2)    as       avg_amount38_enquiryfee                     
,round(max_amount38_enquiryfee                    ,2)    as       max_amount38_enquiryfee                     
,round(sum_cnt38_enquiryfee                       ,2)    as       sum_cnt38_enquiryfee                        
,round(min_amount38_enquiryfee                    ,2)    as       min_amount38_enquiryfee                     
,round(sum_amount10_credit                        ,2)    as       sum_amount10_credit                         
,round(avg_amount10_credit                        ,2)    as       avg_amount10_credit                         
,round(max_amount10_credit                        ,2)    as       max_amount10_credit                         
,round(min_amount10_credit                        ,2)    as       min_amount10_credit                         
,round(sum_cnt10_credit                           ,2)    as       sum_cnt10_credit                            
,round(sum_amount39_enquiryfee                    ,2)    as       sum_amount39_enquiryfee                     
,round(avg_amount39_enquiryfee                    ,2)    as       avg_amount39_enquiryfee                     
,round(max_amount39_enquiryfee                    ,2)    as       max_amount39_enquiryfee                     
,round(sum_cnt39_enquiryfee                       ,2)    as       sum_cnt39_enquiryfee                        
,round(min_amount39_enquiryfee                    ,2)    as       min_amount39_enquiryfee                     
,round(sum_amount49_creditcardrepayments          ,2)    as       sum_amount49_creditcardrepayments           
,round(avg_amount49_creditcardrepayments          ,2)    as       avg_amount49_creditcardrepayments           
,round(max_amount49_creditcardrepayments          ,2)    as    max_amount49_creditcardrepayments              
,round(min_amount49_creditcardrepayments          ,2)    as       min_amount49_creditcardrepayments           
,round(sum_cnt49_creditcardrepayments             ,2)    as       sum_cnt49_creditcardrepayments              
,round(sum_amount11_credit                        ,2)    as    sum_amount11_credit                            
,round(avg_amount11_credit                        ,2)    as       avg_amount11_credit                         
,round(max_amount11_credit                        ,2)    as       max_amount11_credit                         
,round(min_amount11_credit                        ,2)    as       min_amount11_credit                         
,round(sum_cnt11_credit                           ,2)    as       sum_cnt11_credit                            
,round(sum_amount40_enquiryfee                    ,2)    as       sum_amount40_enquiryfee                     
,round(avg_amount40_enquiryfee                    ,2)    as       avg_amount40_enquiryfee                     
,round(max_amount40_enquiryfee                    ,2)    as       max_amount40_enquiryfee                     
,round(min_amount40_enquiryfee                    ,2)    as       min_amount40_enquiryfee                     
,round(sum_cnt40_enquiryfee                       ,2)    as       sum_cnt40_enquiryfee                        
,round(sum_amount50_creditcardrepayments          ,2)    as       sum_amount50_creditcardrepayments           
,round(avg_amount50_creditcardrepayments          ,2)    as       avg_amount50_creditcardrepayments           
,round(max_amount50_creditcardrepayments          ,2)    as    max_amount50_creditcardrepayments              
,round(min_amount50_creditcardrepayments          ,2)    as       min_amount50_creditcardrepayments           
,round(sum_cnt50_creditcardrepayments             ,2)    as       sum_cnt50_creditcardrepayments              
,round(sum_amount12_credit                        ,2)    as       sum_amount12_credit                         
,round(avg_amount12_credit                        ,2)    as    avg_amount12_credit                            
,round(max_amount12_credit                        ,2)    as       max_amount12_credit                         
,round(min_amount12_credit                        ,2)    as       min_amount12_credit                         
,round(sum_cnt12_credit                           ,2)    as       sum_cnt12_credit                            
,round(sum_amount41_enquiryfee                    ,2)    as       sum_amount41_enquiryfee                     
,round(avg_amount41_enquiryfee                    ,2)    as       avg_amount41_enquiryfee                     
,round(max_amount41_enquiryfee                    ,2)    as       max_amount41_enquiryfee                     
,round(sum_cnt41_enquiryfee                       ,2)    as       sum_cnt41_enquiryfee                        
,round(min_amount41_enquiryfee                    ,2)    as       min_amount41_enquiryfee                     
,round(sum_amount51_creditcardrepayments          ,2)    as       sum_amount51_creditcardrepayments           
,round(avg_amount51_creditcardrepayments          ,2)    as       avg_amount51_creditcardrepayments           
,round(max_amount51_creditcardrepayments          ,2)    as       max_amount51_creditcardrepayments           
,round(min_amount51_creditcardrepayments          ,2)    as    min_amount51_creditcardrepayments              
,round(sum_cnt51_creditcardrepayments             ,2)    as       sum_cnt51_creditcardrepayments              
,round(sum_amount42_enquiryfee                    ,2)    as       sum_amount42_enquiryfee                     
,round(avg_amount42_enquiryfee                    ,2)    as       avg_amount42_enquiryfee                     
,round(max_amount42_enquiryfee                    ,2)    as       max_amount42_enquiryfee                     
,round(sum_cnt42_enquiryfee                       ,2)    as       sum_cnt42_enquiryfee                        
,round(min_amount42_enquiryfee                    ,2)    as       min_amount42_enquiryfee                     
,round(sum_amount52_creditcardrepayments          ,2)    as    sum_amount52_creditcardrepayments              
,round(avg_amount52_creditcardrepayments          ,2)    as    avg_amount52_creditcardrepayments              
,round(max_amount52_creditcardrepayments          ,2)    as    max_amount52_creditcardrepayments              
,round(min_amount52_creditcardrepayments          ,2)    as       min_amount52_creditcardrepayments           
,round(sum_cnt52_creditcardrepayments             ,2)    as       sum_cnt52_creditcardrepayments              
,round(sum_amount28_mobilephonerecharge           ,2)    as       sum_amount28_mobilephonerecharge            
,round(avg_amount28_mobilephonerecharge           ,2)    as       avg_amount28_mobilephonerecharge            
,round(max_amount28_mobilephonerecharge           ,2)    as       max_amount28_mobilephonerecharge            
,round(min_amount28_mobilephonerecharge           ,2)    as       min_amount28_mobilephonerecharge            
,round(sum_cnt28_mobilephonerecharge              ,2)    as       sum_cnt28_mobilephonerecharge               
,round(sum_amount27_mobilephonerecharge           ,2)    as       sum_amount27_mobilephonerecharge            
,round(avg_amount27_mobilephonerecharge           ,2)    as       avg_amount27_mobilephonerecharge            
,round(max_amount27_mobilephonerecharge           ,2)    as       max_amount27_mobilephonerecharge            
,round(min_amount27_mobilephonerecharge           ,2)    as       min_amount27_mobilephonerecharge            
,round(sum_cnt27_mobilephonerecharge              ,2)    as       sum_cnt27_mobilephonerecharge               
,round(sum_amount29_mobilephonerecharge           ,2)    as       sum_amount29_mobilephonerecharge            
,round(avg_amount29_mobilephonerecharge           ,2)    as       avg_amount29_mobilephonerecharge            
,round(max_amount29_mobilephonerecharge           ,2)    as       max_amount29_mobilephonerecharge            
,round(min_amount29_mobilephonerecharge           ,2)    as       min_amount29_mobilephonerecharge            
,round(sum_cnt29_mobilephonerecharge              ,2)    as       sum_cnt29_mobilephonerecharge               
,round(sum_amount26_mobilephonerecharge           ,2)    as       sum_amount26_mobilephonerecharge            
,round(avg_amount26_mobilephonerecharge           ,2)    as       avg_amount26_mobilephonerecharge            
,round(max_amount26_mobilephonerecharge           ,2)    as       max_amount26_mobilephonerecharge            
,round(min_amount26_mobilephonerecharge           ,2)    as       min_amount26_mobilephonerecharge            
,round(sum_cnt26_mobilephonerecharge              ,2)    as       sum_cnt26_mobilephonerecharge               
,round(sum_amount25_mobilephonerecharge           ,2)    as       sum_amount25_mobilephonerecharge            
,round(avg_amount25_mobilephonerecharge           ,2)    as       avg_amount25_mobilephonerecharge            
,round(max_amount25_mobilephonerecharge           ,2)    as       max_amount25_mobilephonerecharge            
,round(min_amount25_mobilephonerecharge           ,2)    as       min_amount25_mobilephonerecharge            
,round(sum_cnt25_mobilephonerecharge              ,2)    as       sum_cnt25_mobilephonerecharge               
,round(sum_amount31_mobilephonerecharge           ,2)    as       sum_amount31_mobilephonerecharge            
,round(avg_amount31_mobilephonerecharge           ,2)    as       avg_amount31_mobilephonerecharge            
,round(max_amount31_mobilephonerecharge           ,2)    as       max_amount31_mobilephonerecharge            
,round(min_amount31_mobilephonerecharge           ,2)    as       min_amount31_mobilephonerecharge            
,round(sum_cnt31_mobilephonerecharge              ,2)    as       sum_cnt31_mobilephonerecharge               
,round(sum_amount30_mobilephonerecharge           ,2)    as       sum_amount30_mobilephonerecharge            
,round(avg_amount30_mobilephonerecharge           ,2)    as       avg_amount30_mobilephonerecharge            
,round(max_amount30_mobilephonerecharge           ,2)    as       max_amount30_mobilephonerecharge            
,round(min_amount30_mobilephonerecharge           ,2)    as       min_amount30_mobilephonerecharge            
,round(sum_cnt30_mobilephonerecharge              ,2)    as       sum_cnt30_mobilephonerecharge               
,round(sum_amount24_mobilephonerecharge           ,2)    as       sum_amount24_mobilephonerecharge            
,round(avg_amount24_mobilephonerecharge           ,2)    as       avg_amount24_mobilephonerecharge            
,round(max_amount24_mobilephonerecharge           ,2)    as       max_amount24_mobilephonerecharge            
,round(min_amount24_mobilephonerecharge           ,2)    as       min_amount24_mobilephonerecharge            
,round(sum_cnt24_mobilephonerecharge              ,2)    as       sum_cnt24_mobilephonerecharge               
,round(sum_amount23_mobilephonerecharge           ,2)    as       sum_amount23_mobilephonerecharge            
,round(avg_amount23_mobilephonerecharge           ,2)    as       avg_amount23_mobilephonerecharge            
,round(max_amount23_mobilephonerecharge           ,2)    as       max_amount23_mobilephonerecharge            
,round(min_amount23_mobilephonerecharge           ,2)    as       min_amount23_mobilephonerecharge            
,round(sum_cnt23_mobilephonerecharge              ,2)    as       sum_cnt23_mobilephonerecharge               
,round(sum_amount22_mobilephonerecharge           ,2)    as       sum_amount22_mobilephonerecharge            
,round(avg_amount22_mobilephonerecharge           ,2)    as       avg_amount22_mobilephonerecharge            
,round(max_amount22_mobilephonerecharge           ,2)    as       max_amount22_mobilephonerecharge            
,round(min_amount22_mobilephonerecharge           ,2)    as       min_amount22_mobilephonerecharge            
,round(sum_cnt22_mobilephonerecharge              ,2)    as       sum_cnt22_mobilephonerecharge               
,round(sum_amount32_mobilephonerecharge           ,2)    as       sum_amount32_mobilephonerecharge            
,round(avg_amount32_mobilephonerecharge           ,2)    as       avg_amount32_mobilephonerecharge            
,round(max_amount32_mobilephonerecharge           ,2)    as       max_amount32_mobilephonerecharge            
,round(min_amount32_mobilephonerecharge           ,2)    as       min_amount32_mobilephonerecharge            
,round(sum_cnt32_mobilephonerecharge              ,2)    as       sum_cnt32_mobilephonerecharge               
,round(sum_amount21_mobilephonerecharge           ,2)    as       sum_amount21_mobilephonerecharge            
,round(avg_amount21_mobilephonerecharge           ,2)    as       avg_amount21_mobilephonerecharge            
,round(max_amount21_mobilephonerecharge           ,2)    as       max_amount21_mobilephonerecharge            
,round(min_amount21_mobilephonerecharge           ,2)    as       min_amount21_mobilephonerecharge            
,round(sum_cnt21_mobilephonerecharge              ,2)    as       sum_cnt21_mobilephonerecharge               
,round(sum_amount33_mobilephonerecharge           ,2)    as       sum_amount33_mobilephonerecharge            
,round(avg_amount33_mobilephonerecharge           ,2)    as       avg_amount33_mobilephonerecharge            
,round(max_amount33_mobilephonerecharge           ,2)    as       max_amount33_mobilephonerecharge            
,round(min_amount33_mobilephonerecharge           ,2)    as       min_amount33_mobilephonerecharge            
,round(sum_cnt33_mobilephonerecharge              ,2)    as       sum_cnt33_mobilephonerecharge               
,round(sum_amount20_mobilephonerecharge           ,2)    as    sum_amount20_mobilephonerecharge               
,round(avg_amount20_mobilephonerecharge           ,2)    as       avg_amount20_mobilephonerecharge            
,round(max_amount20_mobilephonerecharge           ,2)    as       max_amount20_mobilephonerecharge            
,round(min_amount20_mobilephonerecharge           ,2)    as       min_amount20_mobilephonerecharge            
,round(sum_cnt20_mobilephonerecharge              ,2)    as       sum_cnt20_mobilephonerecharge               
,round(sum_amount34_mobilephonerecharge           ,2)    as       sum_amount34_mobilephonerecharge            
,round(avg_amount34_mobilephonerecharge           ,2)    as       avg_amount34_mobilephonerecharge            
,round(max_amount34_mobilephonerecharge           ,2)    as       max_amount34_mobilephonerecharge            
,round(min_amount34_mobilephonerecharge           ,2)    as       min_amount34_mobilephonerecharge            
,round(sum_cnt34_mobilephonerecharge              ,2)    as       sum_cnt34_mobilephonerecharge               
,round(sum_amount53_creditcardrepayments          ,2)    as       sum_amount53_creditcardrepayments           
,round(avg_amount53_creditcardrepayments          ,2)    as       avg_amount53_creditcardrepayments           
,round(max_amount53_creditcardrepayments          ,2)    as    max_amount53_creditcardrepayments              
,round(min_amount53_creditcardrepayments          ,2)    as       min_amount53_creditcardrepayments           
,round(sum_cnt53_creditcardrepayments             ,2)    as       sum_cnt53_creditcardrepayments              
,round(sum_amount13_credit                        ,2)    as       sum_amount13_credit                         
,round(avg_amount13_credit                        ,2)    as       avg_amount13_credit                         
,round(max_amount13_credit                        ,2)    as    max_amount13_credit                            
,round(min_amount13_credit                        ,2)    as       min_amount13_credit                         
,round(sum_cnt13_credit                           ,2)    as       sum_cnt13_credit                            
,round(sum_amount19_mobilephonerecharge           ,2)    as       sum_amount19_mobilephonerecharge            
,round(avg_amount19_mobilephonerecharge           ,2)    as       avg_amount19_mobilephonerecharge            
,round(max_amount19_mobilephonerecharge           ,2)    as       max_amount19_mobilephonerecharge            
,round(min_amount19_mobilephonerecharge           ,2)    as       min_amount19_mobilephonerecharge            
,round(sum_cnt19_mobilephonerecharge              ,2)    as       sum_cnt19_mobilephonerecharge               
,round(sum_amount18_mobilephonerecharge           ,2)    as       sum_amount18_mobilephonerecharge            
,round(avg_amount18_mobilephonerecharge           ,2)    as       avg_amount18_mobilephonerecharge            
,round(max_amount18_mobilephonerecharge           ,2)    as       max_amount18_mobilephonerecharge            
,round(min_amount18_mobilephonerecharge           ,2)    as       min_amount18_mobilephonerecharge            
,round(sum_cnt18_mobilephonerecharge              ,2)    as       sum_cnt18_mobilephonerecharge               
,round(sum_amount35_mobilephonerecharge           ,2)    as       sum_amount35_mobilephonerecharge            
,round(avg_amount35_mobilephonerecharge           ,2)    as       avg_amount35_mobilephonerecharge            
,round(max_amount35_mobilephonerecharge           ,2)    as       max_amount35_mobilephonerecharge            
,round(min_amount35_mobilephonerecharge           ,2)    as       min_amount35_mobilephonerecharge            
,round(sum_cnt35_mobilephonerecharge              ,2)    as       sum_cnt35_mobilephonerecharge               
,round(sum_amount43_enquiryfee                    ,2)    as       sum_amount43_enquiryfee                     
,round(avg_amount43_enquiryfee                    ,2)    as       avg_amount43_enquiryfee                     
,round(max_amount43_enquiryfee                    ,2)    as       max_amount43_enquiryfee                     
,round(sum_cnt43_enquiryfee                       ,2)    as        sum_cnt43_enquiryfee                       
,round(min_amount43_enquiryfee                    ,2)    as       min_amount43_enquiryfee                     
,round(sum_amount17_mobilephonerecharge           ,2)    as       sum_amount17_mobilephonerecharge            
,round(avg_amount17_mobilephonerecharge           ,2)    as       avg_amount17_mobilephonerecharge            
,round(max_amount17_mobilephonerecharge           ,2)    as       max_amount17_mobilephonerecharge            
,round(min_amount17_mobilephonerecharge           ,2)    as       min_amount17_mobilephonerecharge            
,round(sum_cnt17_mobilephonerecharge              ,2)    as       sum_cnt17_mobilephonerecharge               
,round(sum_amount36_mobilephonerecharge           ,2)    as       sum_amount36_mobilephonerecharge            
,round(avg_amount36_mobilephonerecharge           ,2)    as       avg_amount36_mobilephonerecharge            
,round(max_amount36_mobilephonerecharge           ,2)    as       max_amount36_mobilephonerecharge            
,round(min_amount36_mobilephonerecharge           ,2)    as       min_amount36_mobilephonerecharge            
,round(sum_cnt36_mobilephonerecharge              ,2)    as       sum_cnt36_mobilephonerecharge               
,round(sum_amount16_mobilephonerecharge           ,2)    as       sum_amount16_mobilephonerecharge            
,round(avg_amount16_mobilephonerecharge           ,2)    as       avg_amount16_mobilephonerecharge            
,round(max_amount16_mobilephonerecharge           ,2)    as       max_amount16_mobilephonerecharge            
,round(min_amount16_mobilephonerecharge           ,2)    as       min_amount16_mobilephonerecharge            
,round(sum_cnt16_mobilephonerecharge              ,2)    as       sum_cnt16_mobilephonerecharge               
,round(sum_amount37_mobilephonerecharge           ,2)    as       sum_amount37_mobilephonerecharge            
,round(avg_amount37_mobilephonerecharge           ,2)    as       avg_amount37_mobilephonerecharge            
,round(max_amount37_mobilephonerecharge           ,2)    as       max_amount37_mobilephonerecharge            
,round(min_amount37_mobilephonerecharge           ,2)    as       min_amount37_mobilephonerecharge            
,round(sum_cnt37_mobilephonerecharge              ,2)    as       sum_cnt37_mobilephonerecharge               
,round(sum_amount38_mobilephonerecharge           ,2)    as       sum_amount38_mobilephonerecharge            
,round(avg_amount38_mobilephonerecharge           ,2)    as       avg_amount38_mobilephonerecharge            
,round(max_amount38_mobilephonerecharge           ,2)    as       max_amount38_mobilephonerecharge            
,round(min_amount38_mobilephonerecharge           ,2)    as       min_amount38_mobilephonerecharge            
,round(sum_cnt38_mobilephonerecharge              ,2)    as       sum_cnt38_mobilephonerecharge               
,round(sum_amount15_mobilephonerecharge           ,2)    as       sum_amount15_mobilephonerecharge            
,round(avg_amount15_mobilephonerecharge           ,2)    as       avg_amount15_mobilephonerecharge            
,round(max_amount15_mobilephonerecharge           ,2)    as       max_amount15_mobilephonerecharge            
,round(min_amount15_mobilephonerecharge           ,2)    as       min_amount15_mobilephonerecharge            
,round(sum_cnt15_mobilephonerecharge              ,2)    as       sum_cnt15_mobilephonerecharge               
,round(sum_amount14_mobilephonerecharge           ,2)    as    sum_amount14_mobilephonerecharge               
,round(avg_amount14_mobilephonerecharge           ,2)    as       avg_amount14_mobilephonerecharge            
,round(max_amount14_mobilephonerecharge           ,2)    as       max_amount14_mobilephonerecharge            
,round(min_amount14_mobilephonerecharge           ,2)    as       min_amount14_mobilephonerecharge            
,round(sum_cnt14_mobilephonerecharge              ,2)    as       sum_cnt14_mobilephonerecharge               
,round(sum_amount54_creditcardrepayments          ,2)    as       sum_amount54_creditcardrepayments           
,round(avg_amount54_creditcardrepayments          ,2)    as       avg_amount54_creditcardrepayments           
,round(max_amount54_creditcardrepayments          ,2)    as    max_amount54_creditcardrepayments              
,round(min_amount54_creditcardrepayments          ,2)    as       min_amount54_creditcardrepayments           
,round(sum_cnt54_creditcardrepayments             ,2)    as       sum_cnt54_creditcardrepayments              
,round(sum_amount39_mobilephonerecharge           ,2)    as       sum_amount39_mobilephonerecharge            
,round(avg_amount39_mobilephonerecharge           ,2)    as       avg_amount39_mobilephonerecharge            
,round(max_amount39_mobilephonerecharge           ,2)    as       max_amount39_mobilephonerecharge            
,round(min_amount39_mobilephonerecharge           ,2)    as       min_amount39_mobilephonerecharge            
,round(sum_cnt39_mobilephonerecharge              ,2)    as       sum_cnt39_mobilephonerecharge               
                                                                                                            
    

from phone_variable_yfq_creditcardrepayments_20170724
where 
  (sum_amount20_creditcardrepayments    <>0
or avg_amount20_creditcardrepayments<>0
or max_amount20_creditcardrepayments<>0
or min_amount20_creditcardrepayments<>0
or sum_cnt20_creditcardrepayments   <>0
or sum_amount19_creditcardrepayments<>0
or avg_amount19_creditcardrepayments<>0
or max_amount19_creditcardrepayments<>0
or min_amount19_creditcardrepayments<>0
or sum_cnt19_creditcardrepayments   <>0
or sum_amount21_creditcardrepayments<>0
or avg_amount21_creditcardrepayments<>0
or max_amount21_creditcardrepayments<>0
or min_amount21_creditcardrepayments<>0
or sum_cnt21_creditcardrepayments   <>0
or sum_amount18_creditcardrepayments<>0
or avg_amount18_creditcardrepayments<>0
or max_amount18_creditcardrepayments<>0
or min_amount18_creditcardrepayments<>0
or sum_cnt18_creditcardrepayments   <>0
or sum_amount22_creditcardrepayments<>0
or avg_amount22_creditcardrepayments<>0
or max_amount22_creditcardrepayments<>0
or min_amount22_creditcardrepayments<>0
or sum_cnt22_creditcardrepayments   <>0
or sum_amount17_creditcardrepayments<>0
or avg_amount17_creditcardrepayments<>0
or max_amount17_creditcardrepayments<>0
or min_amount17_creditcardrepayments<>0
or sum_cnt17_creditcardrepayments   <>0
or sum_amount23_creditcardrepayments<>0
or avg_amount23_creditcardrepayments<>0
or max_amount23_creditcardrepayments<>0
or min_amount23_creditcardrepayments<>0
or sum_cnt23_creditcardrepayments   <>0
or sum_amount16_creditcardrepayments<>0
or avg_amount16_creditcardrepayments<>0
or max_amount16_creditcardrepayments<>0
or min_amount16_creditcardrepayments<>0
or sum_cnt16_creditcardrepayments   <>0
or sum_amount24_creditcardrepayments<>0
or avg_amount24_creditcardrepayments<>0
or max_amount24_creditcardrepayments<>0
or min_amount24_creditcardrepayments<>0
or sum_cnt24_creditcardrepayments   <>0
or sum_amount15_creditcardrepayments<>0
or avg_amount15_creditcardrepayments<>0
or max_amount15_creditcardrepayments<>0
or min_amount15_creditcardrepayments<>0
or sum_cnt15_creditcardrepayments   <>0
or sum_amount25_creditcardrepayments<>0
or avg_amount25_creditcardrepayments<>0
or max_amount25_creditcardrepayments<>0
or min_amount25_creditcardrepayments<>0
or sum_cnt25_creditcardrepayments   <>0
or sum_amount14_creditcardrepayments<>0
or avg_amount14_creditcardrepayments<>0
or max_amount14_creditcardrepayments<>0
or min_amount14_creditcardrepayments<>0
or sum_cnt14_creditcardrepayments   <>0
or sum_amount26_creditcardrepayments<>0
or avg_amount26_creditcardrepayments<>0
or max_amount26_creditcardrepayments<>0
or min_amount26_creditcardrepayments<>0
or sum_cnt26_creditcardrepayments   <>0
or sum_amount13_creditcardrepayments<>0
or avg_amount13_creditcardrepayments<>0
or max_amount13_creditcardrepayments<>0
or min_amount13_creditcardrepayments<>0
or sum_cnt13_creditcardrepayments   <>0
or sum_amount27_creditcardrepayments<>0
or avg_amount27_creditcardrepayments<>0
or max_amount27_creditcardrepayments<>0
or min_amount27_creditcardrepayments<>0
or sum_cnt27_creditcardrepayments   <>0
or sum_amount12_creditcardrepayments<>0
or avg_amount12_creditcardrepayments<>0
or max_amount12_creditcardrepayments<>0
or min_amount12_creditcardrepayments<>0
or sum_cnt12_creditcardrepayments   <>0
or sum_amount28_creditcardrepayments<>0
or avg_amount28_creditcardrepayments<>0
or max_amount28_creditcardrepayments<>0
or min_amount28_creditcardrepayments<>0
or sum_cnt28_creditcardrepayments   <>0
or sum_amount11_creditcardrepayments<>0
or avg_amount11_creditcardrepayments<>0
or max_amount11_creditcardrepayments<>0
or min_amount11_creditcardrepayments<>0
or sum_cnt11_creditcardrepayments   <>0
or sum_amount29_creditcardrepayments<>0
or avg_amount29_creditcardrepayments<>0
or max_amount29_creditcardrepayments<>0
or min_amount29_creditcardrepayments<>0
or sum_cnt29_creditcardrepayments   <>0
or sum_amount10_creditcardrepayments<>0
or avg_amount10_creditcardrepayments<>0
or max_amount10_creditcardrepayments<>0
or min_amount10_creditcardrepayments<>0
or sum_cnt10_creditcardrepayments   <>0
or sum_amount30_creditcardrepayments<>0
or avg_amount30_creditcardrepayments<>0
or max_amount30_creditcardrepayments<>0
or min_amount30_creditcardrepayments<>0
or sum_cnt30_creditcardrepayments   <>0
or sum_amount9_creditcardrepayments <>0
or avg_amount9_creditcardrepayments <>0
or max_amount9_creditcardrepayments <>0
or min_amount9_creditcardrepayments <>0
or sum_cnt9_creditcardrepayments    <>0
or sum_amount31_creditcardrepayments<>0
or avg_amount31_creditcardrepayments<>0
or max_amount31_creditcardrepayments<>0
or min_amount31_creditcardrepayments<>0
or sum_cnt31_creditcardrepayments   <>0
or sum_amount8_creditcardrepayments <>0
or avg_amount8_creditcardrepayments <>0
or max_amount8_creditcardrepayments <>0
or min_amount8_creditcardrepayments <>0
or sum_cnt8_creditcardrepayments    <>0
or sum_amount32_creditcardrepayments<>0
or avg_amount32_creditcardrepayments<>0
or max_amount32_creditcardrepayments<>0
or min_amount32_creditcardrepayments<>0
or sum_cnt32_creditcardrepayments   <>0
or sum_amount7_creditcardrepayments <>0
or avg_amount7_creditcardrepayments <>0
or max_amount7_creditcardrepayments <>0
or min_amount7_creditcardrepayments <>0
or sum_cnt7_creditcardrepayments    <>0
or sum_amount33_creditcardrepayments<>0
or avg_amount33_creditcardrepayments<>0
or max_amount33_creditcardrepayments<>0
or min_amount33_creditcardrepayments<>0
or sum_cnt33_creditcardrepayments   <>0
or sum_amount17_enquiryfee          <>0
or avg_amount17_enquiryfee          <>0
or max_amount17_enquiryfee          <>0
or sum_cnt17_enquiryfee             <>0
or min_amount17_enquiryfee          <>0
or sum_amount6_creditcardrepayments <>0
or avg_amount6_creditcardrepayments <>0
or max_amount6_creditcardrepayments <>0
or min_amount6_creditcardrepayments <>0
or sum_cnt6_creditcardrepayments    <>0
or sum_amount16_enquiryfee          <>0
or avg_amount16_enquiryfee          <>0
or max_amount16_enquiryfee          <>0
or sum_cnt16_enquiryfee             <>0
or min_amount16_enquiryfee          <>0
or sum_amount18_enquiryfee          <>0
or avg_amount18_enquiryfee          <>0
or max_amount18_enquiryfee          <>0
or sum_cnt18_enquiryfee             <>0
or min_amount18_enquiryfee          <>0
or sum_amount34_creditcardrepayments<>0
or avg_amount34_creditcardrepayments<>0
or max_amount34_creditcardrepayments<>0
or min_amount34_creditcardrepayments<>0
or sum_cnt34_creditcardrepayments   <>0
or sum_amount15_enquiryfee          <>0
or avg_amount15_enquiryfee          <>0
or max_amount15_enquiryfee          <>0
or sum_cnt15_enquiryfee             <>0
or min_amount15_enquiryfee          <>0
or sum_amount19_enquiryfee          <>0
or avg_amount19_enquiryfee          <>0
or max_amount19_enquiryfee          <>0
or sum_cnt19_enquiryfee             <>0
or min_amount19_enquiryfee          <>0
or sum_amount20_enquiryfee          <>0
or avg_amount20_enquiryfee          <>0
or max_amount20_enquiryfee          <>0
or sum_cnt20_enquiryfee             <>0
or min_amount20_enquiryfee          <>0
or sum_amount14_enquiryfee          <>0
or avg_amount14_enquiryfee          <>0
or max_amount14_enquiryfee          <>0
or sum_cnt14_enquiryfee             <>0
or min_amount14_enquiryfee          <>0
or sum_amount21_enquiryfee          <>0
or avg_amount21_enquiryfee          <>0
or max_amount21_enquiryfee          <>0
or sum_cnt21_enquiryfee             <>0
or min_amount21_enquiryfee          <>0
or sum_amount13_enquiryfee          <>0
or avg_amount13_enquiryfee          <>0
or max_amount13_enquiryfee          <>0
or sum_cnt13_enquiryfee             <>0
or min_amount13_enquiryfee          <>0
or sum_amount22_enquiryfee          <>0
or avg_amount22_enquiryfee          <>0
or max_amount22_enquiryfee          <>0
or sum_cnt22_enquiryfee             <>0
or min_amount22_enquiryfee          <>0
or sum_amount12_enquiryfee          <>0
or avg_amount12_enquiryfee          <>0
or max_amount12_enquiryfee          <>0
or min_amount12_enquiryfee          <>0
or sum_cnt12_enquiryfee             <>0
or sum_amount35_creditcardrepayments<>0
or avg_amount35_creditcardrepayments<>0
or max_amount35_creditcardrepayments<>0
or min_amount35_creditcardrepayments<>0
or sum_cnt35_creditcardrepayments   <>0
or sum_amount5_creditcardrepayments <>0
or avg_amount5_creditcardrepayments <>0
or max_amount5_creditcardrepayments <>0
or min_amount5_creditcardrepayments <>0
or sum_cnt5_creditcardrepayments    <>0
or sum_amount23_enquiryfee          <>0
or avg_amount23_enquiryfee          <>0
or max_amount23_enquiryfee          <>0
or sum_cnt23_enquiryfee             <>0
or min_amount23_enquiryfee          <>0
or sum_amount11_enquiryfee          <>0
or avg_amount11_enquiryfee          <>0
or max_amount11_enquiryfee          <>0
or min_amount11_enquiryfee          <>0
or sum_cnt11_enquiryfee             <>0
or sum_amount24_enquiryfee          <>0
or avg_amount24_enquiryfee          <>0
or max_amount24_enquiryfee          <>0
or sum_cnt24_enquiryfee             <>0
or min_amount24_enquiryfee          <>0
or sum_amount10_enquiryfee          <>0
or avg_amount10_enquiryfee          <>0
or max_amount10_enquiryfee          <>0
or sum_cnt10_enquiryfee             <>0
or min_amount10_enquiryfee          <>0
or sum_amount36_creditcardrepayments<>0
or avg_amount36_creditcardrepayments<>0
or max_amount36_creditcardrepayments<>0
or min_amount36_creditcardrepayments<>0
or sum_cnt36_creditcardrepayments   <>0
or sum_amount4_creditcardrepayments <>0
or avg_amount4_creditcardrepayments <>0
or max_amount4_creditcardrepayments <>0
or min_amount4_creditcardrepayments <>0
or sum_cnt4_creditcardrepayments    <>0
or sum_amount9_enquiryfee           <>0
or avg_amount9_enquiryfee           <>0
or max_amount9_enquiryfee           <>0
or sum_cnt9_enquiryfee              <>0
or min_amount9_enquiryfee           <>0
or sum_amount25_enquiryfee          <>0
or avg_amount25_enquiryfee          <>0
or max_amount25_enquiryfee          <>0
or sum_cnt25_enquiryfee             <>0
or min_amount25_enquiryfee          <>0
or sum_amount8_enquiryfee           <>0
or avg_amount8_enquiryfee           <>0
or max_amount8_enquiryfee           <>0
or sum_cnt8_enquiryfee              <>0
or min_amount8_enquiryfee           <>0
or sum_amount37_creditcardrepayments<>0
or avg_amount37_creditcardrepayments<>0
or max_amount37_creditcardrepayments<>0
or min_amount37_creditcardrepayments<>0
or sum_cnt37_creditcardrepayments   <>0
or sum_amount3_creditcardrepayments <>0
or avg_amount3_creditcardrepayments <>0
or max_amount3_creditcardrepayments <>0
or min_amount3_creditcardrepayments <>0
or sum_cnt3_creditcardrepayments    <>0
or sum_amount26_enquiryfee          <>0
or avg_amount26_enquiryfee          <>0
or max_amount26_enquiryfee          <>0
or sum_cnt26_enquiryfee             <>0
or min_amount26_enquiryfee          <>0
or sum_amount7_enquiryfee           <>0
or avg_amount7_enquiryfee           <>0
or max_amount7_enquiryfee           <>0
or sum_cnt7_enquiryfee              <>0
or min_amount7_enquiryfee           <>0
or sum_amount27_enquiryfee          <>0
or avg_amount27_enquiryfee          <>0
or max_amount27_enquiryfee          <>0
or sum_cnt27_enquiryfee             <>0
or min_amount27_enquiryfee          <>0
or sum_amount38_creditcardrepayments<>0
or avg_amount38_creditcardrepayments<>0
or max_amount38_creditcardrepayments<>0
or min_amount38_creditcardrepayments<>0
or sum_cnt38_creditcardrepayments   <>0
or sum_amount6_enquiryfee           <>0
or avg_amount6_enquiryfee           <>0
or max_amount6_enquiryfee           <>0
or sum_cnt6_enquiryfee              <>0
or min_amount6_enquiryfee           <>0
or sum_amount2_creditcardrepayments <>0
or avg_amount2_creditcardrepayments <>0
or max_amount2_creditcardrepayments <>0
or min_amount2_creditcardrepayments <>0
or sum_cnt2_creditcardrepayments    <>0
or sum_amount1_creditcardrepayments <>0
or avg_amount1_creditcardrepayments <>0
or max_amount1_creditcardrepayments <>0
or min_amount1_creditcardrepayments <>0
or sum_cnt1_creditcardrepayments    <>0
or sum_amount28_enquiryfee          <>0
or avg_amount28_enquiryfee          <>0
or max_amount28_enquiryfee          <>0
or sum_cnt28_enquiryfee             <>0
or min_amount28_enquiryfee          <>0
or sum_amount5_enquiryfee           <>0
or avg_amount5_enquiryfee           <>0
or max_amount5_enquiryfee           <>0
or sum_cnt5_enquiryfee              <>0
or min_amount5_enquiryfee           <>0
or sum_amount39_creditcardrepayments<>0
or avg_amount39_creditcardrepayments<>0
or max_amount39_creditcardrepayments<>0
or min_amount39_creditcardrepayments<>0
or sum_cnt39_creditcardrepayments   <>0
or sum_amount4_enquiryfee           <>0
or avg_amount4_enquiryfee           <>0
or max_amount4_enquiryfee           <>0
or sum_cnt4_enquiryfee              <>0
or min_amount4_enquiryfee           <>0
or sum_amount29_enquiryfee          <>0
or avg_amount29_enquiryfee          <>0
or max_amount29_enquiryfee          <>0
or sum_cnt29_enquiryfee             <>0
or min_amount29_enquiryfee          <>0
or sum_amount3_enquiryfee           <>0
or avg_amount3_enquiryfee           <>0
or max_amount3_enquiryfee           <>0
or min_amount3_enquiryfee           <>0
or sum_cnt3_enquiryfee              <>0
or sum_amount40_creditcardrepayments<>0
or avg_amount40_creditcardrepayments<>0
or max_amount40_creditcardrepayments<>0
or min_amount40_creditcardrepayments<>0
or sum_cnt40_creditcardrepayments   <>0
or sum_amount1_credit               <>0
or avg_amount1_credit               <>0
or max_amount1_credit               <>0
or min_amount1_credit               <>0
or sum_cnt1_credit                  <>0
or sum_amount30_enquiryfee          <>0
or avg_amount30_enquiryfee          <>0
or max_amount30_enquiryfee          <>0
or sum_cnt30_enquiryfee             <>0
or min_amount30_enquiryfee          <>0
or sum_amount2_enquiryfee           <>0
or avg_amount2_enquiryfee           <>0
or max_amount2_enquiryfee           <>0
or sum_cnt2_enquiryfee              <>0
or min_amount2_enquiryfee           <>0
or sum_amount1_enquiryfee           <>0
or avg_amount1_enquiryfee           <>0
or max_amount1_enquiryfee           <>0
or sum_cnt1_enquiryfee              <>0
or min_amount1_enquiryfee           <>0
or sum_amount41_creditcardrepayments<>0
or avg_amount41_creditcardrepayments<>0
or max_amount41_creditcardrepayments<>0
or min_amount41_creditcardrepayments<>0
or sum_cnt41_creditcardrepayments   <>0
or sum_amount2_credit               <>0
or avg_amount2_credit               <>0
or max_amount2_credit               <>0
or min_amount2_credit               <>0
or sum_cnt2_credit                  <>0
or sum_amount31_enquiryfee          <>0
or avg_amount31_enquiryfee          <>0
or max_amount31_enquiryfee          <>0
or sum_cnt31_enquiryfee             <>0
or min_amount31_enquiryfee          <>0
or sum_amount3_credit               <>0
or avg_amount3_credit               <>0
or max_amount3_credit               <>0
or min_amount3_credit               <>0
or sum_cnt3_credit                  <>0
or sum_amount42_creditcardrepayments<>0
or avg_amount42_creditcardrepayments<>0
or max_amount42_creditcardrepayments<>0
or min_amount42_creditcardrepayments<>0
or sum_cnt42_creditcardrepayments   <>0
or sum_amount4_credit               <>0
or avg_amount4_credit               <>0
or max_amount4_credit               <>0
or min_amount4_credit               <>0
or sum_cnt4_credit                  <>0
or sum_amount32_enquiryfee          <>0
or avg_amount32_enquiryfee          <>0
or max_amount32_enquiryfee          <>0
or sum_cnt32_enquiryfee             <>0
or min_amount32_enquiryfee          <>0
or sum_amount5_credit               <>0
or avg_amount5_credit               <>0
or max_amount5_credit               <>0
or min_amount5_credit               <>0
or sum_cnt5_credit                  <>0
or sum_amount43_creditcardrepayments<>0
or avg_amount43_creditcardrepayments<>0
or max_amount43_creditcardrepayments<>0
or min_amount43_creditcardrepayments<>0
or sum_cnt43_creditcardrepayments   <>0
or sum_amount6_credit               <>0
or avg_amount6_credit               <>0
or max_amount6_credit               <>0
or min_amount6_credit               <>0
or sum_cnt6_credit                  <>0
or sum_amount33_enquiryfee          <>0
or avg_amount33_enquiryfee          <>0
or max_amount33_enquiryfee          <>0
or sum_cnt33_enquiryfee             <>0
or min_amount33_enquiryfee          <>0
or sum_amount44_creditcardrepayments<>0
or avg_amount44_creditcardrepayments<>0
or max_amount44_creditcardrepayments<>0
or min_amount44_creditcardrepayments<>0
or sum_cnt44_creditcardrepayments   <>0
or sum_amount34_enquiryfee          <>0
or avg_amount34_enquiryfee          <>0
or max_amount34_enquiryfee          <>0
or sum_cnt34_enquiryfee             <>0
or min_amount34_enquiryfee          <>0
or sum_amount45_creditcardrepayments<>0
or avg_amount45_creditcardrepayments<>0
or max_amount45_creditcardrepayments<>0
or min_amount45_creditcardrepayments<>0
or sum_cnt45_creditcardrepayments   <>0
or sum_amount7_credit               <>0
or avg_amount7_credit               <>0
or max_amount7_credit               <>0
or min_amount7_credit               <>0
or sum_cnt7_credit                  <>0
or sum_amount35_enquiryfee          <>0
or avg_amount35_enquiryfee          <>0
or max_amount35_enquiryfee          <>0
or sum_cnt35_enquiryfee             <>0
or min_amount35_enquiryfee          <>0
or sum_amount46_creditcardrepayments<>0
or avg_amount46_creditcardrepayments<>0
or max_amount46_creditcardrepayments<>0
or min_amount46_creditcardrepayments<>0
or sum_cnt46_creditcardrepayments    <>0   
or sum_amount8_credit                <>0   
or avg_amount8_credit                <>0   
or max_amount8_credit                <>0   
or min_amount8_credit                <>0   
or sum_cnt8_credit                   <>0   
or sum_amount36_enquiryfee           <>0   
or avg_amount36_enquiryfee           <>0   
or max_amount36_enquiryfee           <>0   
or sum_cnt36_enquiryfee              <>0   
or min_amount36_enquiryfee           <>0   
or sum_amount47_creditcardrepayments <>0   
or avg_amount47_creditcardrepayments <>0   
or max_amount47_creditcardrepayments <>0   
or min_amount47_creditcardrepayments <>0   
or sum_cnt47_creditcardrepayments    <>0   
or sum_amount9_credit                <>0   
or avg_amount9_credit                <>0   
or max_amount9_credit                <>0   
or min_amount9_credit                <>0   
or sum_cnt9_credit                   <>0   
or sum_amount37_enquiryfee          <>0
or avg_amount37_enquiryfee          <>0
or max_amount37_enquiryfee          <>0
or sum_cnt37_enquiryfee             <>0
or min_amount37_enquiryfee          <>0
or sum_amount48_creditcardrepayments<>0
or avg_amount48_creditcardrepayments<>0
or max_amount48_creditcardrepayments<>0
or min_amount48_creditcardrepayments<>0
or sum_cnt48_creditcardrepayments   <>0
or sum_amount38_enquiryfee          <>0
or avg_amount38_enquiryfee          <>0
or max_amount38_enquiryfee          <>0
or sum_cnt38_enquiryfee             <>0
or min_amount38_enquiryfee          <>0
or sum_amount10_credit              <>0
or avg_amount10_credit              <>0
or max_amount10_credit              <>0
or min_amount10_credit              <>0
or sum_cnt10_credit                 <>0
or sum_amount39_enquiryfee          <>0
or avg_amount39_enquiryfee          <>0
or max_amount39_enquiryfee          <>0
or sum_cnt39_enquiryfee             <>0
or min_amount39_enquiryfee          <>0
or sum_amount49_creditcardrepayments<>0
or avg_amount49_creditcardrepayments<>0
or max_amount49_creditcardrepayments<>0
or min_amount49_creditcardrepayments<>0
or sum_cnt49_creditcardrepayments   <>0
or sum_amount11_credit              <>0
or avg_amount11_credit              <>0
or max_amount11_credit              <>0
or min_amount11_credit              <>0
or sum_cnt11_credit                 <>0
or sum_amount40_enquiryfee          <>0
or avg_amount40_enquiryfee          <>0
or max_amount40_enquiryfee          <>0
or min_amount40_enquiryfee          <>0
or sum_cnt40_enquiryfee             <>0
or sum_amount50_creditcardrepayments<>0
or avg_amount50_creditcardrepayments<>0
or max_amount50_creditcardrepayments<>0
or min_amount50_creditcardrepayments<>0
or sum_cnt50_creditcardrepayments   <>0
or sum_amount12_credit              <>0
or avg_amount12_credit              <>0
or max_amount12_credit              <>0
or min_amount12_credit              <>0
or sum_cnt12_credit                 <>0
or sum_amount41_enquiryfee          <>0
or avg_amount41_enquiryfee          <>0
or max_amount41_enquiryfee          <>0
or sum_cnt41_enquiryfee             <>0
or min_amount41_enquiryfee          <>0
or sum_amount51_creditcardrepayments<>0
or avg_amount51_creditcardrepayments<>0
or max_amount51_creditcardrepayments<>0
or min_amount51_creditcardrepayments<>0
or sum_cnt51_creditcardrepayments   <>0
or sum_amount42_enquiryfee           <>0   
or avg_amount42_enquiryfee           <>0   
or max_amount42_enquiryfee           <>0   
or sum_cnt42_enquiryfee              <>0   
or min_amount42_enquiryfee           <>0   
or sum_amount52_creditcardrepayments <>0   
or avg_amount52_creditcardrepayments <>0   
or max_amount52_creditcardrepayments <>0   
or min_amount52_creditcardrepayments <>0   
or sum_cnt52_creditcardrepayments    <>0   
or sum_amount28_mobilephonerecharge  <>0   
or avg_amount28_mobilephonerecharge  <>0   
or max_amount28_mobilephonerecharge  <>0   
or min_amount28_mobilephonerecharge  <>0   
or sum_cnt28_mobilephonerecharge     <>0   
or sum_amount27_mobilephonerecharge  <>0   
or avg_amount27_mobilephonerecharge  <>0   
or max_amount27_mobilephonerecharge  <>0   
or min_amount27_mobilephonerecharge  <>0   
or sum_cnt27_mobilephonerecharge     <>0   
or sum_amount29_mobilephonerecharge  <>0   
or avg_amount29_mobilephonerecharge  <>0
or max_amount29_mobilephonerecharge  <>0
or min_amount29_mobilephonerecharge  <>0
or sum_cnt29_mobilephonerecharge     <>0
or sum_amount26_mobilephonerecharge  <>0
or avg_amount26_mobilephonerecharge  <>0
or max_amount26_mobilephonerecharge  <>0
or min_amount26_mobilephonerecharge  <>0
or sum_cnt26_mobilephonerecharge     <>0
or sum_amount25_mobilephonerecharge  <>0
or avg_amount25_mobilephonerecharge  <>0
or max_amount25_mobilephonerecharge  <>0
or min_amount25_mobilephonerecharge  <>0
or sum_cnt25_mobilephonerecharge     <>0
or sum_amount31_mobilephonerecharge  <>0
or avg_amount31_mobilephonerecharge  <>0
or max_amount31_mobilephonerecharge  <>0
or min_amount31_mobilephonerecharge  <>0
or sum_cnt31_mobilephonerecharge     <>0
or sum_amount30_mobilephonerecharge  <>0
or avg_amount30_mobilephonerecharge  <>0
or max_amount30_mobilephonerecharge  <>0
or min_amount30_mobilephonerecharge  <>0
or sum_cnt30_mobilephonerecharge     <>0
or sum_amount24_mobilephonerecharge  <>0
or avg_amount24_mobilephonerecharge  <>0
or max_amount24_mobilephonerecharge  <>0
or min_amount24_mobilephonerecharge  <>0
or sum_cnt24_mobilephonerecharge     <>0
or sum_amount23_mobilephonerecharge  <>0
or avg_amount23_mobilephonerecharge  <>0
or max_amount23_mobilephonerecharge  <>0
or min_amount23_mobilephonerecharge  <>0
or sum_cnt23_mobilephonerecharge     <>0
or sum_amount22_mobilephonerecharge  <>0
or avg_amount22_mobilephonerecharge  <>0
or max_amount22_mobilephonerecharge  <>0
or min_amount22_mobilephonerecharge  <>0
or sum_cnt22_mobilephonerecharge     <>0
or sum_amount32_mobilephonerecharge  <>0
or avg_amount32_mobilephonerecharge  <>0
or max_amount32_mobilephonerecharge  <>0
or min_amount32_mobilephonerecharge  <>0
or sum_cnt32_mobilephonerecharge     <>0
or sum_amount21_mobilephonerecharge  <>0
or avg_amount21_mobilephonerecharge  <>0
or max_amount21_mobilephonerecharge  <>0
or min_amount21_mobilephonerecharge  <>0
or sum_cnt21_mobilephonerecharge     <>0
or sum_amount33_mobilephonerecharge  <>0
or avg_amount33_mobilephonerecharge  <>0
or max_amount33_mobilephonerecharge  <>0
or min_amount33_mobilephonerecharge  <>0
or sum_cnt33_mobilephonerecharge     <>0
or sum_amount20_mobilephonerecharge  <>0
or avg_amount20_mobilephonerecharge  <>0
or max_amount20_mobilephonerecharge  <>0
or min_amount20_mobilephonerecharge  <>0
or sum_cnt20_mobilephonerecharge     <>0
or sum_amount34_mobilephonerecharge  <>0
or avg_amount34_mobilephonerecharge   <>0   
or max_amount34_mobilephonerecharge  <>0   
or min_amount34_mobilephonerecharge  <>0   
or sum_cnt34_mobilephonerecharge     <>0   
or sum_amount53_creditcardrepayments <>0   
or avg_amount53_creditcardrepayments <>0   
or max_amount53_creditcardrepayments <>0   
or min_amount53_creditcardrepayments <>0   
or sum_cnt53_creditcardrepayments    <>0   
or sum_amount13_credit               <>0   
or avg_amount13_credit               <>0   
or max_amount13_credit               <>0   
or min_amount13_credit               <>0   
or sum_cnt13_credit                  <>0   
or sum_amount19_mobilephonerecharge  <>0   
or avg_amount19_mobilephonerecharge  <>0   
or max_amount19_mobilephonerecharge  <>0   
or min_amount19_mobilephonerecharge  <>0   
or sum_cnt19_mobilephonerecharge     <>0   
or sum_amount18_mobilephonerecharge  <>0   
or avg_amount18_mobilephonerecharge  <>0   
or max_amount18_mobilephonerecharge  <>0
or min_amount18_mobilephonerecharge  <>0
or sum_cnt18_mobilephonerecharge     <>0
or sum_amount35_mobilephonerecharge  <>0
or avg_amount35_mobilephonerecharge  <>0
or max_amount35_mobilephonerecharge  <>0
or min_amount35_mobilephonerecharge  <>0
or sum_cnt35_mobilephonerecharge     <>0
or sum_amount43_enquiryfee           <>0
or avg_amount43_enquiryfee           <>0
or max_amount43_enquiryfee           <>0
or sum_cnt43_enquiryfee              <>0
or min_amount43_enquiryfee           <>0
or sum_amount17_mobilephonerecharge  <>0
or avg_amount17_mobilephonerecharge  <>0
or max_amount17_mobilephonerecharge  <>0
or min_amount17_mobilephonerecharge  <>0
or sum_cnt17_mobilephonerecharge     <>0
or sum_amount36_mobilephonerecharge  <>0
or avg_amount36_mobilephonerecharge  <>0
or max_amount36_mobilephonerecharge  <>0
or min_amount36_mobilephonerecharge  <>0
or sum_cnt36_mobilephonerecharge     <>0
or sum_amount16_mobilephonerecharge  <>0
or avg_amount16_mobilephonerecharge  <>0
or max_amount16_mobilephonerecharge  <>0
or min_amount16_mobilephonerecharge  <>0
or sum_cnt16_mobilephonerecharge     <>0
or sum_amount37_mobilephonerecharge  <>0
or avg_amount37_mobilephonerecharge  <>0
or max_amount37_mobilephonerecharge  <>0
or min_amount37_mobilephonerecharge  <>0
or sum_cnt37_mobilephonerecharge     <>0
or sum_amount38_mobilephonerecharge  <>0
or avg_amount38_mobilephonerecharge  <>0   
or max_amount38_mobilephonerecharge  <>0   
or min_amount38_mobilephonerecharge  <>0   
or sum_cnt38_mobilephonerecharge     <>0   
or sum_amount15_mobilephonerecharge  <>0   
or avg_amount15_mobilephonerecharge  <>0   
or max_amount15_mobilephonerecharge  <>0   
or min_amount15_mobilephonerecharge  <>0   
or sum_cnt15_mobilephonerecharge     <>0   
or sum_amount14_mobilephonerecharge  <>0   
or avg_amount14_mobilephonerecharge  <>0   
or max_amount14_mobilephonerecharge  <>0   
or min_amount14_mobilephonerecharge  <>0   
or sum_cnt14_mobilephonerecharge     <>0   
or sum_amount54_creditcardrepayments <>0   
or avg_amount54_creditcardrepayments <>0   
or max_amount54_creditcardrepayments <>0   
or min_amount54_creditcardrepayments <>0   
or sum_cnt54_creditcardrepayments    <>0   
or sum_amount39_mobilephonerecharge  <>0   
or avg_amount39_mobilephonerecharge  <>0   
or max_amount39_mobilephonerecharge  <>0
or min_amount39_mobilephonerecharge  <>0
or sum_cnt39_mobilephonerecharge     <>0

 )
;




drop table tnh_gbdt_0727;
create EXTERNAL table  tnh_gbdt_0727  (phone string,score string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location 'hdfs://ns1/user/luhuamin/20170727/tnh/predictionAndLabels/';


drop table yfq_gbdt_0727;
create EXTERNAL table  yfq_gbdt_0727  ( phone string,score string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location 'hdfs://ns1/user/luhuamin/20170727/yfq/predictionAndLabels/';


drop table tnh_gbdt_0727_replace;
create table tnh_gbdt_0727_replace as 
select regexp_replace(phone,'\\(','') phone,regexp_replace(score,'\\)','') score
from tnh_gbdt_0727;


drop table yfq_gbdt_0727_replace;
create  table yfq_gbdt_0727_replace as 
select regexp_replace(phone,'\\(','') phone,regexp_replace(score,'\\)','') score
from yfq_gbdt_0727;


------------------------------------------------------------------------------------------
-- 手机号码跟合同号关联
create table lkl_card_contract_mobile as 
select  a.mobile,b.contract_no from  
r_creditloan.s_c_apply_user a  
inner join   creditloan.s_c_loan_contract b where a.year=2017 and a.month=07 and a.day=22
and b.year=2017 and b.month=07 and b.day=22 and a.id=b.apply_id 
group by a.mobile,b.contract_no
;

drop table lkl_card_tnh_contract_727;
create table lkl_card_tnh_contract_727 as 
select   c.* ,bb.contract_no 
from  
lkl_card_score.tnh_gbdt_0727_replace c
left join lkl_card_contract_mobile bb   
on bb.mobile=c.phone 
;

drop table lkl_card_yfq_contract_727;
create table lkl_card_yfq_contract_727 as 
select   c.* ,bb.contract_no 
from  
lkl_card_score.yfq_gbdt_0727_replace c
left join lkl_card_contract_mobile bb   
on bb.mobile=c.phone 
;

--过滤信贷导入的合同号
drop table lkl_card_tnh_contract_result_0727;

create table lkl_card_tnh_contract_result_0727 
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' as 
select a.phone,a.contract_no,a.score from lkl_card_tnh_contract_727 a 
join tnh_all b on a.contract_no =b.contract_no;
;

drop table lkl_card_yfq_contract_result_0727;

create table lkl_card_yfq_contract_result_0727 
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' as  
select a.phone,a.contract_no,a.score from lkl_card_yfq_contract_727 a 
join yfq_all b on a.contract_no =b.contract_no;
;

---------------------------------------------------------------

drop table tnh_all;
create EXTERNAL table  tnh_all  (contract_no string,procid string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location 'hdfs://ns1/user/luhuamin/tnh_gb/tnh_all/';


drop table yfq_all;
create EXTERNAL table  yfq_all  (contract_no string,procid string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location 'hdfs://ns1/user/luhuamin/yfq_gb/yfq_all/';                 




hadoop fs -getmerge /user/hive/warehouse/lkl_card_score.db/lkl_card_yfq_contract_result_0724/*             /user/luhuamin/lkl_card_yfq_contract_result_0724.csv                      
hadoop fs -getmerge /user/hive/warehouse/lkl_card_score.db/lkl_card_tnh_contract_result_0724/*           /user/luhuamin/lkl_card_tnh_contract_result_0724.csv          