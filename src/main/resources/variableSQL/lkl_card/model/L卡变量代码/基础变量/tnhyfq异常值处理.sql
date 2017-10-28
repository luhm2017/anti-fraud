drop table phone_variable_tnh_creditcardrepayments_train_tc;
create table phone_variable_tnh_creditcardrepayments_train_tc as 
select 
--label               
phone               
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount1_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount1_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount1_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount1_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt1_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount5_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount5_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount5_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount5_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt5_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount2_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount2_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount2_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount2_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt2_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount4_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount4_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount4_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount4_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt4_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount6_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount6_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount6_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount6_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt6_creditcardrepayments    
,nvl(sum_amount7_creditcardrepayments ,0)     sum_amount7_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)     avg_amount7_creditcardrepayments 
,nvl(max_amount7_creditcardrepayments ,0)     max_amount7_creditcardrepayments 
,nvl(min_amount7_creditcardrepayments ,0)     min_amount7_creditcardrepayments 
,nvl(sum_cnt7_creditcardrepayments    ,0)     sum_cnt7_creditcardrepayments    
,nvl(sum_amount9_creditcardrepayments ,0)     sum_amount9_creditcardrepayments 
,nvl(avg_amount9_creditcardrepayments ,0)     avg_amount9_creditcardrepayments 
,nvl(max_amount9_creditcardrepayments ,0)     max_amount9_creditcardrepayments 
,nvl(min_amount9_creditcardrepayments ,0)     min_amount9_creditcardrepayments 
,nvl(sum_cnt9_creditcardrepayments    ,0)     sum_cnt9_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)     sum_amount3_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)     avg_amount3_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)     max_amount3_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)     min_amount3_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)     sum_cnt3_creditcardrepayments    
,nvl(sum_amount8_creditcardrepayments ,0)     sum_amount8_creditcardrepayments 
,nvl(avg_amount8_creditcardrepayments ,0)     avg_amount8_creditcardrepayments 
,nvl(max_amount8_creditcardrepayments ,0)     max_amount8_creditcardrepayments 
,nvl(min_amount8_creditcardrepayments ,0)     min_amount8_creditcardrepayments 
,nvl(sum_cnt8_creditcardrepayments    ,0)     sum_cnt8_creditcardrepayments    
,nvl(sum_amount10_creditcardrepayments,0)     sum_amount10_creditcardrepayments
,nvl(avg_amount10_creditcardrepayments,0)     avg_amount10_creditcardrepayments
,nvl(max_amount10_creditcardrepayments,0)     max_amount10_creditcardrepayments
,nvl(min_amount10_creditcardrepayments,0)     min_amount10_creditcardrepayments
,nvl(sum_cnt10_creditcardrepayments   ,0)     sum_cnt10_creditcardrepayments   
,nvl(sum_amount11_creditcardrepayments,0)     sum_amount11_creditcardrepayments
,nvl(avg_amount11_creditcardrepayments,0)     avg_amount11_creditcardrepayments
,nvl(max_amount11_creditcardrepayments,0)     max_amount11_creditcardrepayments
,nvl(min_amount11_creditcardrepayments,0)     min_amount11_creditcardrepayments
,nvl(sum_cnt11_creditcardrepayments   ,0)     sum_cnt11_creditcardrepayments   
,nvl(sum_amount12_creditcardrepayments,0)     sum_amount12_creditcardrepayments
,nvl(avg_amount12_creditcardrepayments,0)     avg_amount12_creditcardrepayments
,nvl(max_amount12_creditcardrepayments,0)     max_amount12_creditcardrepayments
,nvl(min_amount12_creditcardrepayments,0)     min_amount12_creditcardrepayments
,nvl(sum_cnt12_creditcardrepayments   ,0)     sum_cnt12_creditcardrepayments   
,nvl(sum_amount14_creditcardrepayments,0)     sum_amount14_creditcardrepayments
,nvl(avg_amount14_creditcardrepayments,0)     avg_amount14_creditcardrepayments
,nvl(max_amount14_creditcardrepayments,0)     max_amount14_creditcardrepayments
,nvl(min_amount14_creditcardrepayments,0)     min_amount14_creditcardrepayments
,nvl(sum_cnt14_creditcardrepayments   ,0)     sum_cnt14_creditcardrepayments   
,nvl(sum_amount13_creditcardrepayments,0)     sum_amount13_creditcardrepayments
,nvl(avg_amount13_creditcardrepayments,0)     avg_amount13_creditcardrepayments
,nvl(max_amount13_creditcardrepayments,0)     max_amount13_creditcardrepayments
,nvl(min_amount13_creditcardrepayments,0)     min_amount13_creditcardrepayments
,nvl(sum_cnt13_creditcardrepayments   ,0)     sum_cnt13_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)     sum_amount15_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)     avg_amount15_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)     max_amount15_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)     min_amount15_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)     sum_cnt15_creditcardrepayments   
,nvl(sum_amount16_creditcardrepayments,0)     sum_amount16_creditcardrepayments
,nvl(avg_amount16_creditcardrepayments,0)     avg_amount16_creditcardrepayments
,nvl(max_amount16_creditcardrepayments,0)     max_amount16_creditcardrepayments
,nvl(min_amount16_creditcardrepayments,0)     min_amount16_creditcardrepayments
,nvl(sum_cnt16_creditcardrepayments   ,0)     sum_cnt16_creditcardrepayments   
,nvl(sum_amount17_creditcardrepayments,0)     sum_amount17_creditcardrepayments
,nvl(avg_amount17_creditcardrepayments,0)     avg_amount17_creditcardrepayments
,nvl(max_amount17_creditcardrepayments,0)     max_amount17_creditcardrepayments
,nvl(min_amount17_creditcardrepayments,0)     min_amount17_creditcardrepayments
,nvl(sum_cnt17_creditcardrepayments   ,0)     sum_cnt17_creditcardrepayments   
,nvl(sum_amount18_creditcardrepayments,0)     sum_amount18_creditcardrepayments
,nvl(avg_amount18_creditcardrepayments,0)     avg_amount18_creditcardrepayments
,nvl(max_amount18_creditcardrepayments,0)     max_amount18_creditcardrepayments
,nvl(min_amount18_creditcardrepayments,0)     min_amount18_creditcardrepayments
,nvl(sum_cnt18_creditcardrepayments   ,0)     sum_cnt18_creditcardrepayments   
,nvl(sum_amount19_creditcardrepayments,0)     sum_amount19_creditcardrepayments
,nvl(avg_amount19_creditcardrepayments,0)     avg_amount19_creditcardrepayments
,nvl(max_amount19_creditcardrepayments,0)     max_amount19_creditcardrepayments
,nvl(min_amount19_creditcardrepayments,0)     min_amount19_creditcardrepayments
,nvl(sum_cnt19_creditcardrepayments   ,0)     sum_cnt19_creditcardrepayments   
,nvl(sum_amount20_creditcardrepayments,0)     sum_amount20_creditcardrepayments
,nvl(avg_amount20_creditcardrepayments,0)     avg_amount20_creditcardrepayments
,nvl(max_amount20_creditcardrepayments,0)     max_amount20_creditcardrepayments
,nvl(min_amount20_creditcardrepayments,0)     min_amount20_creditcardrepayments
,nvl(sum_cnt20_creditcardrepayments   ,0)     sum_cnt20_creditcardrepayments   
,nvl(sum_amount21_creditcardrepayments,0)     sum_amount21_creditcardrepayments
,nvl(avg_amount21_creditcardrepayments,0)     avg_amount21_creditcardrepayments
,nvl(max_amount21_creditcardrepayments,0)     max_amount21_creditcardrepayments
,nvl(min_amount21_creditcardrepayments,0)     min_amount21_creditcardrepayments
,nvl(sum_cnt21_creditcardrepayments   ,0)     sum_cnt21_creditcardrepayments   
,nvl(sum_amount22_creditcardrepayments,0)     sum_amount22_creditcardrepayments
,nvl(avg_amount22_creditcardrepayments,0)     avg_amount22_creditcardrepayments
,nvl(max_amount22_creditcardrepayments,0)     max_amount22_creditcardrepayments
,nvl(min_amount22_creditcardrepayments,0)     min_amount22_creditcardrepayments
,nvl(sum_cnt22_creditcardrepayments   ,0)     sum_cnt22_creditcardrepayments   
,nvl(sum_amount23_creditcardrepayments,0)     sum_amount23_creditcardrepayments
,nvl(avg_amount23_creditcardrepayments,0)     avg_amount23_creditcardrepayments
,nvl(max_amount23_creditcardrepayments,0)     max_amount23_creditcardrepayments
,nvl(min_amount23_creditcardrepayments,0)     min_amount23_creditcardrepayments
,nvl(sum_cnt23_creditcardrepayments   ,0)     sum_cnt23_creditcardrepayments   
,nvl(sum_amount24_creditcardrepayments,0)     sum_amount24_creditcardrepayments
,nvl(avg_amount24_creditcardrepayments,0)     avg_amount24_creditcardrepayments
,nvl(max_amount24_creditcardrepayments,0)     max_amount24_creditcardrepayments
,nvl(min_amount24_creditcardrepayments,0)     min_amount24_creditcardrepayments
,nvl(sum_cnt24_creditcardrepayments   ,0)     sum_cnt24_creditcardrepayments   
,nvl(sum_amount25_creditcardrepayments,0)     sum_amount25_creditcardrepayments
,nvl(avg_amount25_creditcardrepayments,0)     avg_amount25_creditcardrepayments
,nvl(max_amount25_creditcardrepayments,0)     max_amount25_creditcardrepayments
,nvl(min_amount25_creditcardrepayments,0)     min_amount25_creditcardrepayments
,nvl(sum_cnt25_creditcardrepayments   ,0)     sum_cnt25_creditcardrepayments   
,nvl(sum_amount26_creditcardrepayments,0)     sum_amount26_creditcardrepayments
,nvl(avg_amount26_creditcardrepayments,0)     avg_amount26_creditcardrepayments
,nvl(max_amount26_creditcardrepayments,0)     max_amount26_creditcardrepayments
,nvl(min_amount26_creditcardrepayments,0)     min_amount26_creditcardrepayments
,nvl(sum_cnt26_creditcardrepayments   ,0)     sum_cnt26_creditcardrepayments   
,nvl(sum_amount27_creditcardrepayments,0)     sum_amount27_creditcardrepayments
,nvl(avg_amount27_creditcardrepayments,0)     avg_amount27_creditcardrepayments
,nvl(max_amount27_creditcardrepayments,0)     max_amount27_creditcardrepayments
,nvl(min_amount27_creditcardrepayments,0)     min_amount27_creditcardrepayments
,nvl(sum_cnt27_creditcardrepayments   ,0)     sum_cnt27_creditcardrepayments   
,nvl(sum_amount28_creditcardrepayments,0)     sum_amount28_creditcardrepayments
,nvl(avg_amount28_creditcardrepayments,0)     avg_amount28_creditcardrepayments
,nvl(max_amount28_creditcardrepayments,0)     max_amount28_creditcardrepayments
,nvl(min_amount28_creditcardrepayments,0)     min_amount28_creditcardrepayments
,nvl(sum_cnt28_creditcardrepayments   ,0)     sum_cnt28_creditcardrepayments   
,nvl(sum_amount29_creditcardrepayments,0)     sum_amount29_creditcardrepayments
,nvl(avg_amount29_creditcardrepayments,0)     avg_amount29_creditcardrepayments
,nvl(max_amount29_creditcardrepayments,0)     max_amount29_creditcardrepayments
,nvl(min_amount29_creditcardrepayments,0)     min_amount29_creditcardrepayments
,nvl(sum_cnt29_creditcardrepayments   ,0)     sum_cnt29_creditcardrepayments   
,nvl(sum_amount30_creditcardrepayments,0)     sum_amount30_creditcardrepayments
,nvl(avg_amount30_creditcardrepayments,0)     avg_amount30_creditcardrepayments
,nvl(max_amount30_creditcardrepayments,0)     max_amount30_creditcardrepayments
,nvl(min_amount30_creditcardrepayments,0)     min_amount30_creditcardrepayments
,nvl(sum_cnt30_creditcardrepayments   ,0)     sum_cnt30_creditcardrepayments   
,nvl(sum_amount31_creditcardrepayments,0)     sum_amount31_creditcardrepayments
,nvl(avg_amount31_creditcardrepayments,0)     avg_amount31_creditcardrepayments
,nvl(max_amount31_creditcardrepayments,0)     max_amount31_creditcardrepayments
,nvl(min_amount31_creditcardrepayments,0)     min_amount31_creditcardrepayments
,nvl(sum_cnt31_creditcardrepayments   ,0)     sum_cnt31_creditcardrepayments   
,nvl(sum_amount32_creditcardrepayments,0)     sum_amount32_creditcardrepayments
,nvl(avg_amount32_creditcardrepayments,0)     avg_amount32_creditcardrepayments
,nvl(max_amount32_creditcardrepayments,0)     max_amount32_creditcardrepayments
,nvl(min_amount32_creditcardrepayments,0)     min_amount32_creditcardrepayments
,nvl(sum_cnt32_creditcardrepayments   ,0)     sum_cnt32_creditcardrepayments   
,nvl(sum_amount33_creditcardrepayments,0)     sum_amount33_creditcardrepayments
,nvl(avg_amount33_creditcardrepayments,0)     avg_amount33_creditcardrepayments
,nvl(max_amount33_creditcardrepayments,0)     max_amount33_creditcardrepayments
,nvl(min_amount33_creditcardrepayments,0)     min_amount33_creditcardrepayments
,nvl(sum_cnt33_creditcardrepayments   ,0)     sum_cnt33_creditcardrepayments   
,nvl(sum_amount34_creditcardrepayments,0)     sum_amount34_creditcardrepayments
,nvl(avg_amount34_creditcardrepayments,0)     avg_amount34_creditcardrepayments
,nvl(max_amount34_creditcardrepayments,0)     max_amount34_creditcardrepayments
,nvl(min_amount34_creditcardrepayments,0)     min_amount34_creditcardrepayments
,nvl(sum_cnt34_creditcardrepayments   ,0)     sum_cnt34_creditcardrepayments   
,nvl(sum_amount35_creditcardrepayments,0)     sum_amount35_creditcardrepayments
,nvl(avg_amount35_creditcardrepayments,0)     avg_amount35_creditcardrepayments
,nvl(max_amount35_creditcardrepayments,0)     max_amount35_creditcardrepayments
,nvl(min_amount35_creditcardrepayments,0)     min_amount35_creditcardrepayments
,nvl(sum_cnt35_creditcardrepayments   ,0)     sum_cnt35_creditcardrepayments   
,nvl(sum_amount36_creditcardrepayments,0)     sum_amount36_creditcardrepayments
,nvl(avg_amount36_creditcardrepayments,0)     avg_amount36_creditcardrepayments
,nvl(max_amount36_creditcardrepayments,0)     max_amount36_creditcardrepayments
,nvl(min_amount36_creditcardrepayments,0)     min_amount36_creditcardrepayments
,nvl(sum_cnt36_creditcardrepayments   ,0)     sum_cnt36_creditcardrepayments   
,nvl(sum_amount37_creditcardrepayments,0)     sum_amount37_creditcardrepayments
,nvl(avg_amount37_creditcardrepayments,0)     avg_amount37_creditcardrepayments
,nvl(max_amount37_creditcardrepayments,0)     max_amount37_creditcardrepayments
,nvl(min_amount37_creditcardrepayments,0)     min_amount37_creditcardrepayments
,nvl(sum_cnt37_creditcardrepayments   ,0)     sum_cnt37_creditcardrepayments   
,nvl(sum_amount38_creditcardrepayments,0)     sum_amount38_creditcardrepayments
,nvl(avg_amount38_creditcardrepayments,0)     avg_amount38_creditcardrepayments
,nvl(max_amount38_creditcardrepayments,0)     max_amount38_creditcardrepayments
,nvl(min_amount38_creditcardrepayments,0)     min_amount38_creditcardrepayments
,nvl(sum_cnt38_creditcardrepayments   ,0)     sum_cnt38_creditcardrepayments   
,nvl(sum_amount39_creditcardrepayments,0)     sum_amount39_creditcardrepayments
,nvl(avg_amount39_creditcardrepayments,0)     avg_amount39_creditcardrepayments
,nvl(max_amount39_creditcardrepayments,0)     max_amount39_creditcardrepayments
,nvl(min_amount39_creditcardrepayments,0)     min_amount39_creditcardrepayments
,nvl(sum_cnt39_creditcardrepayments   ,0)     sum_cnt39_creditcardrepayments   
,nvl(sum_amount40_creditcardrepayments,0)     sum_amount40_creditcardrepayments
,nvl(avg_amount40_creditcardrepayments,0)     avg_amount40_creditcardrepayments
,nvl(max_amount40_creditcardrepayments,0)     max_amount40_creditcardrepayments
,nvl(min_amount40_creditcardrepayments,0)     min_amount40_creditcardrepayments
,nvl(sum_cnt40_creditcardrepayments   ,0)     sum_cnt40_creditcardrepayments   
,nvl(sum_amount41_creditcardrepayments,0)     sum_amount41_creditcardrepayments
,nvl(avg_amount41_creditcardrepayments,0)     avg_amount41_creditcardrepayments
,nvl(max_amount41_creditcardrepayments,0)     max_amount41_creditcardrepayments
,nvl(min_amount41_creditcardrepayments,0)     min_amount41_creditcardrepayments
,nvl(sum_cnt41_creditcardrepayments   ,0)     sum_cnt41_creditcardrepayments   
,nvl(sum_amount42_creditcardrepayments,0)     sum_amount42_creditcardrepayments
,nvl(avg_amount42_creditcardrepayments,0)     avg_amount42_creditcardrepayments
,nvl(max_amount42_creditcardrepayments,0)     max_amount42_creditcardrepayments
,nvl(min_amount42_creditcardrepayments,0)     min_amount42_creditcardrepayments
,nvl(sum_cnt42_creditcardrepayments   ,0)     sum_cnt42_creditcardrepayments   
,nvl(sum_amount43_creditcardrepayments,0)     sum_amount43_creditcardrepayments
,nvl(avg_amount43_creditcardrepayments,0)     avg_amount43_creditcardrepayments
,nvl(max_amount43_creditcardrepayments,0)     max_amount43_creditcardrepayments
,nvl(min_amount43_creditcardrepayments,0)     min_amount43_creditcardrepayments
,nvl(sum_cnt43_creditcardrepayments   ,0)     sum_cnt43_creditcardrepayments   
,nvl(sum_amount44_creditcardrepayments,0)     sum_amount44_creditcardrepayments
,nvl(avg_amount44_creditcardrepayments,0)     avg_amount44_creditcardrepayments
,nvl(max_amount44_creditcardrepayments,0)     max_amount44_creditcardrepayments
,nvl(min_amount44_creditcardrepayments,0)     min_amount44_creditcardrepayments
,nvl(sum_cnt44_creditcardrepayments   ,0)     sum_cnt44_creditcardrepayments   
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount1_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount1_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount1_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount1_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt1_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount2_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount2_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount2_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount2_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt2_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount3_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount3_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount3_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount3_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt3_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount4_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount4_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount4_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount4_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt4_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount5_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount5_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount5_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount5_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt5_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount6_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount6_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount6_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     min_amount6_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt6_enquiryfee              
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount7_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount7_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount7_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)     sum_cnt7_enquiryfee              
,nvl(sum_cnt8_enquiryfee              ,0)     min_amount7_enquiryfee           
,nvl(sum_amount8_enquiryfee           ,0)     sum_amount8_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)     avg_amount8_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)     max_amount8_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)     sum_cnt8_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)     min_amount8_enquiryfee           
,nvl(sum_amount9_enquiryfee           ,0)     sum_amount9_enquiryfee           
,nvl(avg_amount9_enquiryfee           ,0)     avg_amount9_enquiryfee           
,nvl(max_amount9_enquiryfee           ,0)     max_amount9_enquiryfee           
,nvl(min_amount9_enquiryfee           ,0)     min_amount9_enquiryfee           
,nvl(sum_cnt9_enquiryfee              ,0)     sum_cnt9_enquiryfee              
,nvl(sum_amount10_enquiryfee          ,0)     sum_amount10_enquiryfee          
,nvl(avg_amount10_enquiryfee          ,0)     avg_amount10_enquiryfee          
,nvl(max_amount10_enquiryfee          ,0)     max_amount10_enquiryfee          
,nvl(min_amount10_enquiryfee          ,0)     min_amount10_enquiryfee          
,nvl(sum_cnt10_enquiryfee             ,0)     sum_cnt10_enquiryfee             
,nvl(sum_amount11_enquiryfee          ,0)     sum_amount11_enquiryfee          
,nvl(avg_amount11_enquiryfee          ,0)     avg_amount11_enquiryfee          
,nvl(max_amount11_enquiryfee          ,0)     max_amount11_enquiryfee          
,nvl(min_amount11_enquiryfee          ,0)     min_amount11_enquiryfee          
,nvl(sum_cnt11_enquiryfee             ,0)     sum_cnt11_enquiryfee             
,nvl(sum_amount12_enquiryfee          ,0)     sum_amount12_enquiryfee          
,nvl(avg_amount12_enquiryfee          ,0)     avg_amount12_enquiryfee          
,nvl(max_amount12_enquiryfee          ,0)     max_amount12_enquiryfee          
,nvl(min_amount12_enquiryfee          ,0)     min_amount12_enquiryfee          
,nvl(sum_cnt12_enquiryfee             ,0)     sum_cnt12_enquiryfee             
,nvl(sum_amount13_enquiryfee          ,0)     sum_amount13_enquiryfee          
,nvl(avg_amount13_enquiryfee          ,0)     avg_amount13_enquiryfee          
,nvl(max_amount13_enquiryfee          ,0)     max_amount13_enquiryfee          
,nvl(min_amount13_enquiryfee          ,0)     min_amount13_enquiryfee          
,nvl(sum_cnt13_enquiryfee             ,0)     sum_cnt13_enquiryfee             
,nvl(sum_amount14_enquiryfee          ,0)     sum_amount14_enquiryfee          
,nvl(avg_amount14_enquiryfee          ,0)     avg_amount14_enquiryfee          
,nvl(max_amount14_enquiryfee          ,0)     max_amount14_enquiryfee          
,nvl(min_amount14_enquiryfee          ,0)     min_amount14_enquiryfee          
,nvl(sum_cnt14_enquiryfee             ,0)     sum_cnt14_enquiryfee             
,nvl(sum_amount15_enquiryfee          ,0)     sum_amount15_enquiryfee          
,nvl(avg_amount15_enquiryfee          ,0)     avg_amount15_enquiryfee          
,nvl(max_amount15_enquiryfee          ,0)     max_amount15_enquiryfee          
,nvl(sum_cnt15_enquiryfee             ,0)     sum_cnt15_enquiryfee             
,nvl(min_amount15_enquiryfee          ,0)     min_amount15_enquiryfee          
,nvl(sum_amount16_enquiryfee          ,0)     sum_amount16_enquiryfee          
,nvl(avg_amount16_enquiryfee          ,0)     avg_amount16_enquiryfee          
,nvl(max_amount16_enquiryfee          ,0)     max_amount16_enquiryfee          
,nvl(min_amount16_enquiryfee          ,0)     min_amount16_enquiryfee          
,nvl(sum_cnt16_enquiryfee             ,0)     sum_cnt16_enquiryfee             
,nvl(sum_amount17_enquiryfee          ,0)     sum_amount17_enquiryfee          
,nvl(avg_amount17_enquiryfee          ,0)     avg_amount17_enquiryfee          
,nvl(max_amount17_enquiryfee          ,0)     max_amount17_enquiryfee          
,nvl(min_amount17_enquiryfee          ,0)     min_amount17_enquiryfee          
,nvl(sum_cnt17_enquiryfee             ,0)     sum_cnt17_enquiryfee             
,nvl(sum_amount18_enquiryfee          ,0)     sum_amount18_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)     avg_amount18_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)     max_amount18_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)     sum_cnt18_enquiryfee             
,nvl(min_amount18_enquiryfee          ,0)     min_amount18_enquiryfee          
,nvl(sum_amount19_enquiryfee          ,0)     sum_amount19_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)     avg_amount19_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)     max_amount19_enquiryfee          
,nvl(min_amount19_enquiryfee          ,0)     min_amount19_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)     sum_cnt19_enquiryfee             
,nvl(sum_amount20_enquiryfee          ,0)     sum_amount20_enquiryfee          
,nvl(avg_amount20_enquiryfee          ,0)     avg_amount20_enquiryfee          
,nvl(max_amount20_enquiryfee          ,0)     max_amount20_enquiryfee          
,nvl(min_amount20_enquiryfee          ,0)     min_amount20_enquiryfee          
,nvl(sum_cnt20_enquiryfee             ,0)     sum_cnt20_enquiryfee             
,nvl(sum_amount21_enquiryfee          ,0)     sum_amount21_enquiryfee          
,nvl(avg_amount21_enquiryfee          ,0)     avg_amount21_enquiryfee          
,nvl(max_amount21_enquiryfee          ,0)     max_amount21_enquiryfee          
,nvl(sum_cnt21_enquiryfee             ,0)     sum_cnt21_enquiryfee             
,nvl(min_amount21_enquiryfee          ,0)     min_amount21_enquiryfee          
,nvl(sum_amount22_enquiryfee          ,0)     sum_amount22_enquiryfee          
,nvl(avg_amount22_enquiryfee          ,0)     avg_amount22_enquiryfee          
,nvl(max_amount22_enquiryfee          ,0)     max_amount22_enquiryfee          
,nvl(min_amount22_enquiryfee          ,0)     min_amount22_enquiryfee          
,nvl(sum_cnt22_enquiryfee             ,0)     sum_cnt22_enquiryfee             
,nvl(sum_amount23_enquiryfee          ,0)     sum_amount23_enquiryfee          
,nvl(avg_amount23_enquiryfee          ,0)     avg_amount23_enquiryfee          
,nvl(max_amount23_enquiryfee          ,0)     max_amount23_enquiryfee          
,nvl(min_amount23_enquiryfee          ,0)     min_amount23_enquiryfee          
,nvl(sum_cnt23_enquiryfee             ,0)     sum_cnt23_enquiryfee             
,nvl(sum_amount24_enquiryfee          ,0)     sum_amount24_enquiryfee          
,nvl(avg_amount24_enquiryfee          ,0)     avg_amount24_enquiryfee          
,nvl(max_amount24_enquiryfee          ,0)     max_amount24_enquiryfee          
,nvl(min_amount24_enquiryfee          ,0)     min_amount24_enquiryfee          
,nvl(sum_cnt24_enquiryfee             ,0)     sum_cnt24_enquiryfee             
,nvl(sum_amount25_enquiryfee          ,0)     sum_amount25_enquiryfee          
,nvl(avg_amount25_enquiryfee          ,0)     avg_amount25_enquiryfee          
,nvl(max_amount25_enquiryfee          ,0)     max_amount25_enquiryfee          
,nvl(sum_cnt25_enquiryfee             ,0)     sum_cnt25_enquiryfee             
,nvl(min_amount25_enquiryfee          ,0)     min_amount25_enquiryfee          
,nvl(sum_amount26_enquiryfee          ,0)     sum_amount26_enquiryfee          
,nvl(avg_amount26_enquiryfee          ,0)     avg_amount26_enquiryfee          
,nvl(max_amount26_enquiryfee          ,0)     max_amount26_enquiryfee          
,nvl(min_amount26_enquiryfee          ,0)     min_amount26_enquiryfee          
,nvl(sum_cnt26_enquiryfee             ,0)     sum_cnt26_enquiryfee             
,nvl(sum_amount27_enquiryfee          ,0)     sum_amount27_enquiryfee          
,nvl(avg_amount27_enquiryfee          ,0)     avg_amount27_enquiryfee          
,nvl(max_amount27_enquiryfee          ,0)     max_amount27_enquiryfee          
,nvl(min_amount27_enquiryfee          ,0)     min_amount27_enquiryfee          
,nvl(sum_cnt27_enquiryfee             ,0)     sum_cnt27_enquiryfee             
,nvl(sum_amount28_enquiryfee          ,0)     sum_amount28_enquiryfee          
,nvl(avg_amount28_enquiryfee          ,0)     avg_amount28_enquiryfee          
,nvl(max_amount28_enquiryfee          ,0)     max_amount28_enquiryfee          
,nvl(min_amount28_enquiryfee          ,0)     min_amount28_enquiryfee          
,nvl(sum_cnt28_enquiryfee             ,0)     sum_cnt28_enquiryfee             
,nvl(sum_amount29_enquiryfee          ,0)     sum_amount29_enquiryfee          
,nvl(avg_amount29_enquiryfee          ,0)     avg_amount29_enquiryfee          
,nvl(max_amount29_enquiryfee          ,0)     max_amount29_enquiryfee          
,nvl(min_amount29_enquiryfee          ,0)     min_amount29_enquiryfee          
,nvl(sum_cnt29_enquiryfee             ,0)     sum_cnt29_enquiryfee             
,nvl(sum_amount30_enquiryfee          ,0)     sum_amount30_enquiryfee          
,nvl(avg_amount30_enquiryfee          ,0)     avg_amount30_enquiryfee          
,nvl(max_amount30_enquiryfee          ,0)     max_amount30_enquiryfee          
,nvl(min_amount30_enquiryfee          ,0)     min_amount30_enquiryfee          
,nvl(sum_cnt30_enquiryfee             ,0)     sum_cnt30_enquiryfee             
,nvl(sum_amount31_enquiryfee          ,0)     sum_amount31_enquiryfee          
,nvl(avg_amount31_enquiryfee          ,0)     avg_amount31_enquiryfee          
,nvl(max_amount31_enquiryfee          ,0)     max_amount31_enquiryfee          
,nvl(min_amount31_enquiryfee          ,0)     min_amount31_enquiryfee          
,nvl(sum_cnt31_enquiryfee             ,0)     sum_cnt31_enquiryfee             
,nvl(sum_amount32_enquiryfee          ,0)     sum_amount32_enquiryfee          
,nvl(avg_amount32_enquiryfee          ,0)     avg_amount32_enquiryfee          
,nvl(max_amount32_enquiryfee          ,0)     max_amount32_enquiryfee          
,nvl(sum_cnt32_enquiryfee             ,0)     sum_cnt32_enquiryfee             
,nvl(min_amount32_enquiryfee          ,0)     min_amount32_enquiryfee          
,nvl(sum_amount33_enquiryfee          ,0)     sum_amount33_enquiryfee          
,nvl(avg_amount33_enquiryfee          ,0)     avg_amount33_enquiryfee          
,nvl(max_amount33_enquiryfee          ,0)     max_amount33_enquiryfee          
,nvl(sum_cnt33_enquiryfee             ,0)     sum_cnt33_enquiryfee             
,nvl(min_amount33_enquiryfee          ,0)     min_amount33_enquiryfee          
     
from phone_variable_yfq_tnh_2

;

drop table phone_variable_yfq_creditcardrepayments_train_tc;
create table phone_variable_yfq_creditcardrepayments_train_tc as 
select 
--label
phone
,nvl(sum_amount20_creditcardrepayments,0)  sum_amount20_creditcardrepayments
,nvl(avg_amount20_creditcardrepayments,0)  avg_amount20_creditcardrepayments
,nvl(max_amount20_creditcardrepayments,0)  max_amount20_creditcardrepayments
,nvl(min_amount20_creditcardrepayments,0)  min_amount20_creditcardrepayments
,nvl(sum_cnt20_creditcardrepayments   ,0)  sum_cnt20_creditcardrepayments   
,nvl(sum_amount19_creditcardrepayments,0)  sum_amount19_creditcardrepayments
,nvl(avg_amount19_creditcardrepayments,0)  avg_amount19_creditcardrepayments
,nvl(max_amount19_creditcardrepayments,0)  max_amount19_creditcardrepayments
,nvl(min_amount19_creditcardrepayments,0)  min_amount19_creditcardrepayments
,nvl(sum_cnt19_creditcardrepayments   ,0)  sum_cnt19_creditcardrepayments   
,nvl(sum_amount21_creditcardrepayments,0)  sum_amount21_creditcardrepayments
,nvl(avg_amount21_creditcardrepayments,0)  avg_amount21_creditcardrepayments
,nvl(max_amount21_creditcardrepayments,0)  max_amount21_creditcardrepayments
,nvl(min_amount21_creditcardrepayments,0)  min_amount21_creditcardrepayments
,nvl(sum_cnt21_creditcardrepayments   ,0)  sum_cnt21_creditcardrepayments   
,nvl(sum_amount18_creditcardrepayments,0)  sum_amount18_creditcardrepayments
,nvl(avg_amount18_creditcardrepayments,0)  avg_amount18_creditcardrepayments
,nvl(max_amount18_creditcardrepayments,0)  max_amount18_creditcardrepayments
,nvl(min_amount18_creditcardrepayments,0)  min_amount18_creditcardrepayments
,nvl(sum_cnt18_creditcardrepayments   ,0)  sum_cnt18_creditcardrepayments   
,nvl(sum_amount22_creditcardrepayments,0)  sum_amount22_creditcardrepayments
,nvl(avg_amount22_creditcardrepayments,0)  avg_amount22_creditcardrepayments
,nvl(max_amount22_creditcardrepayments,0)  max_amount22_creditcardrepayments
,nvl(min_amount22_creditcardrepayments,0)  min_amount22_creditcardrepayments
,nvl(sum_cnt22_creditcardrepayments   ,0)  sum_cnt22_creditcardrepayments   
,nvl(sum_amount17_creditcardrepayments,0)  sum_amount17_creditcardrepayments
,nvl(avg_amount17_creditcardrepayments,0)  avg_amount17_creditcardrepayments
,nvl(max_amount17_creditcardrepayments,0)  max_amount17_creditcardrepayments
,nvl(min_amount17_creditcardrepayments,0)  min_amount17_creditcardrepayments
,nvl(sum_cnt17_creditcardrepayments   ,0)  sum_cnt17_creditcardrepayments   
,nvl(sum_amount23_creditcardrepayments,0)  sum_amount23_creditcardrepayments
,nvl(avg_amount23_creditcardrepayments,0)  avg_amount23_creditcardrepayments
,nvl(max_amount23_creditcardrepayments,0)  max_amount23_creditcardrepayments
,nvl(min_amount23_creditcardrepayments,0)  min_amount23_creditcardrepayments
,nvl(sum_cnt23_creditcardrepayments   ,0)  sum_cnt23_creditcardrepayments   
,nvl(sum_amount16_creditcardrepayments,0)  sum_amount16_creditcardrepayments
,nvl(avg_amount16_creditcardrepayments,0)  avg_amount16_creditcardrepayments
,nvl(max_amount16_creditcardrepayments,0)  max_amount16_creditcardrepayments
,nvl(min_amount16_creditcardrepayments,0)  min_amount16_creditcardrepayments
,nvl(sum_cnt16_creditcardrepayments   ,0)  sum_cnt16_creditcardrepayments   
,nvl(sum_amount24_creditcardrepayments,0)  sum_amount24_creditcardrepayments
,nvl(avg_amount24_creditcardrepayments,0)  avg_amount24_creditcardrepayments
,nvl(max_amount24_creditcardrepayments,0)  max_amount24_creditcardrepayments
,nvl(min_amount24_creditcardrepayments,0)  min_amount24_creditcardrepayments
,nvl(sum_cnt24_creditcardrepayments   ,0)  sum_cnt24_creditcardrepayments   
,nvl(sum_amount15_creditcardrepayments,0)  sum_amount15_creditcardrepayments
,nvl(avg_amount15_creditcardrepayments,0)  avg_amount15_creditcardrepayments
,nvl(max_amount15_creditcardrepayments,0)  max_amount15_creditcardrepayments
,nvl(min_amount15_creditcardrepayments,0)  min_amount15_creditcardrepayments
,nvl(sum_cnt15_creditcardrepayments   ,0)  sum_cnt15_creditcardrepayments   
,nvl(sum_amount25_creditcardrepayments,0)  sum_amount25_creditcardrepayments
,nvl(avg_amount25_creditcardrepayments,0)  avg_amount25_creditcardrepayments
,nvl(max_amount25_creditcardrepayments,0)  max_amount25_creditcardrepayments
,nvl(min_amount25_creditcardrepayments,0)  min_amount25_creditcardrepayments
,nvl(sum_cnt25_creditcardrepayments   ,0)  sum_cnt25_creditcardrepayments   
,nvl(sum_amount14_creditcardrepayments,0)  sum_amount14_creditcardrepayments
,nvl(avg_amount14_creditcardrepayments,0)  avg_amount14_creditcardrepayments
,nvl(max_amount14_creditcardrepayments,0)  max_amount14_creditcardrepayments
,nvl(min_amount14_creditcardrepayments,0)  min_amount14_creditcardrepayments
,nvl(sum_cnt14_creditcardrepayments   ,0)  sum_cnt14_creditcardrepayments   
,nvl(sum_amount26_creditcardrepayments,0)  sum_amount26_creditcardrepayments
,nvl(avg_amount26_creditcardrepayments,0)  avg_amount26_creditcardrepayments
,nvl(max_amount26_creditcardrepayments,0)  max_amount26_creditcardrepayments
,nvl(min_amount26_creditcardrepayments,0)  min_amount26_creditcardrepayments
,nvl(sum_cnt26_creditcardrepayments   ,0)  sum_cnt26_creditcardrepayments   
,nvl(sum_amount13_creditcardrepayments,0)  sum_amount13_creditcardrepayments
,nvl(avg_amount13_creditcardrepayments,0)  avg_amount13_creditcardrepayments
,nvl(max_amount13_creditcardrepayments,0)  max_amount13_creditcardrepayments
,nvl(min_amount13_creditcardrepayments,0)  min_amount13_creditcardrepayments
,nvl(sum_cnt13_creditcardrepayments   ,0)  sum_cnt13_creditcardrepayments   
,nvl(sum_amount27_creditcardrepayments,0)  sum_amount27_creditcardrepayments
,nvl(avg_amount27_creditcardrepayments,0)  avg_amount27_creditcardrepayments
,nvl(max_amount27_creditcardrepayments,0)  max_amount27_creditcardrepayments
,nvl(min_amount27_creditcardrepayments,0)  min_amount27_creditcardrepayments
,nvl(sum_cnt27_creditcardrepayments   ,0)  sum_cnt27_creditcardrepayments   
,nvl(sum_amount12_creditcardrepayments,0)  sum_amount12_creditcardrepayments
,nvl(avg_amount12_creditcardrepayments,0)  avg_amount12_creditcardrepayments
,nvl(max_amount12_creditcardrepayments,0)  max_amount12_creditcardrepayments
,nvl(min_amount12_creditcardrepayments,0)  min_amount12_creditcardrepayments
,nvl(sum_cnt12_creditcardrepayments   ,0)  sum_cnt12_creditcardrepayments   
,nvl(sum_amount28_creditcardrepayments,0)  sum_amount28_creditcardrepayments
,nvl(avg_amount28_creditcardrepayments,0)  avg_amount28_creditcardrepayments
,nvl(max_amount28_creditcardrepayments,0)  max_amount28_creditcardrepayments
,nvl(min_amount28_creditcardrepayments,0)  min_amount28_creditcardrepayments
,nvl(sum_cnt28_creditcardrepayments   ,0)  sum_cnt28_creditcardrepayments   
,nvl(sum_amount11_creditcardrepayments,0)  sum_amount11_creditcardrepayments
,nvl(avg_amount11_creditcardrepayments,0)  avg_amount11_creditcardrepayments
,nvl(max_amount11_creditcardrepayments,0)  max_amount11_creditcardrepayments
,nvl(min_amount11_creditcardrepayments,0)  min_amount11_creditcardrepayments
,nvl(sum_cnt11_creditcardrepayments   ,0)  sum_cnt11_creditcardrepayments   
,nvl(sum_amount29_creditcardrepayments,0)  sum_amount29_creditcardrepayments
,nvl(avg_amount29_creditcardrepayments,0)  avg_amount29_creditcardrepayments
,nvl(max_amount29_creditcardrepayments,0)  max_amount29_creditcardrepayments
,nvl(min_amount29_creditcardrepayments,0)  min_amount29_creditcardrepayments
,nvl(sum_cnt29_creditcardrepayments   ,0)  sum_cnt29_creditcardrepayments   
,nvl(sum_amount10_creditcardrepayments,0)  sum_amount10_creditcardrepayments
,nvl(avg_amount10_creditcardrepayments,0)  avg_amount10_creditcardrepayments
,nvl(max_amount10_creditcardrepayments,0)  max_amount10_creditcardrepayments
,nvl(min_amount10_creditcardrepayments,0)  min_amount10_creditcardrepayments
,nvl(sum_cnt10_creditcardrepayments   ,0)  sum_cnt10_creditcardrepayments   
,nvl(sum_amount30_creditcardrepayments,0)  sum_amount30_creditcardrepayments
,nvl(avg_amount30_creditcardrepayments,0)  avg_amount30_creditcardrepayments
,nvl(max_amount30_creditcardrepayments,0)  max_amount30_creditcardrepayments
,nvl(min_amount30_creditcardrepayments,0)  min_amount30_creditcardrepayments
,nvl(sum_cnt30_creditcardrepayments   ,0)  sum_cnt30_creditcardrepayments   
,nvl(sum_amount9_creditcardrepayments ,0)  sum_amount9_creditcardrepayments 
,nvl(avg_amount9_creditcardrepayments ,0)  avg_amount9_creditcardrepayments 
,nvl(max_amount9_creditcardrepayments ,0)  max_amount9_creditcardrepayments 
,nvl(min_amount9_creditcardrepayments ,0)  min_amount9_creditcardrepayments 
,nvl(sum_cnt9_creditcardrepayments    ,0)  sum_cnt9_creditcardrepayments    
,nvl(sum_amount31_creditcardrepayments,0)  sum_amount31_creditcardrepayments
,nvl(avg_amount31_creditcardrepayments,0)  avg_amount31_creditcardrepayments
,nvl(max_amount31_creditcardrepayments,0)  max_amount31_creditcardrepayments
,nvl(min_amount31_creditcardrepayments,0)  min_amount31_creditcardrepayments
,nvl(sum_cnt31_creditcardrepayments   ,0)  sum_cnt31_creditcardrepayments   
,nvl(sum_amount8_creditcardrepayments ,0)  sum_amount8_creditcardrepayments 
,nvl(avg_amount8_creditcardrepayments ,0)  avg_amount8_creditcardrepayments 
,nvl(max_amount8_creditcardrepayments ,0)  max_amount8_creditcardrepayments 
,nvl(min_amount8_creditcardrepayments ,0)  min_amount8_creditcardrepayments 
,nvl(sum_cnt8_creditcardrepayments    ,0)  sum_cnt8_creditcardrepayments    
,nvl(sum_amount32_creditcardrepayments,0)  sum_amount32_creditcardrepayments
,nvl(avg_amount32_creditcardrepayments,0)  avg_amount32_creditcardrepayments
,nvl(max_amount32_creditcardrepayments,0)  max_amount32_creditcardrepayments
,nvl(min_amount32_creditcardrepayments,0)  min_amount32_creditcardrepayments
,nvl(sum_cnt32_creditcardrepayments   ,0)  sum_cnt32_creditcardrepayments   
,nvl(sum_amount7_creditcardrepayments ,0)  sum_amount7_creditcardrepayments 
,nvl(avg_amount7_creditcardrepayments ,0)  avg_amount7_creditcardrepayments 
,nvl(max_amount7_creditcardrepayments ,0)  max_amount7_creditcardrepayments 
,nvl(min_amount7_creditcardrepayments ,0)  min_amount7_creditcardrepayments 
,nvl(sum_cnt7_creditcardrepayments    ,0)  sum_cnt7_creditcardrepayments    
,nvl(sum_amount33_creditcardrepayments,0)  sum_amount33_creditcardrepayments
,nvl(avg_amount33_creditcardrepayments,0)  avg_amount33_creditcardrepayments
,nvl(max_amount33_creditcardrepayments,0)  max_amount33_creditcardrepayments
,nvl(min_amount33_creditcardrepayments,0)  min_amount33_creditcardrepayments
,nvl(sum_cnt33_creditcardrepayments   ,0)  sum_cnt33_creditcardrepayments   
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount6_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount6_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount6_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount6_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt6_creditcardrepayments    
,nvl(sum_amount34_creditcardrepayments,0)  sum_amount34_creditcardrepayments
,nvl(avg_amount34_creditcardrepayments,0)  avg_amount34_creditcardrepayments
,nvl(max_amount34_creditcardrepayments,0)  max_amount34_creditcardrepayments
,nvl(min_amount34_creditcardrepayments,0)  min_amount34_creditcardrepayments
,nvl(sum_cnt34_creditcardrepayments   ,0)  sum_cnt34_creditcardrepayments   
,nvl(sum_amount35_creditcardrepayments,0)  sum_amount35_creditcardrepayments
,nvl(avg_amount35_creditcardrepayments,0)  avg_amount35_creditcardrepayments
,nvl(max_amount35_creditcardrepayments,0)  max_amount35_creditcardrepayments
,nvl(min_amount35_creditcardrepayments,0)  min_amount35_creditcardrepayments
,nvl(sum_cnt35_creditcardrepayments   ,0)  sum_cnt35_creditcardrepayments   
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount5_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount5_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount5_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount5_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt5_creditcardrepayments    
,nvl(sum_amount36_creditcardrepayments,0)  sum_amount36_creditcardrepayments
,nvl(avg_amount36_creditcardrepayments,0)  avg_amount36_creditcardrepayments
,nvl(max_amount36_creditcardrepayments,0)  max_amount36_creditcardrepayments
,nvl(min_amount36_creditcardrepayments,0)  min_amount36_creditcardrepayments
,nvl(sum_cnt36_creditcardrepayments   ,0)  sum_cnt36_creditcardrepayments   
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount4_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount4_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount4_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount4_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt4_creditcardrepayments    
,nvl(sum_amount37_creditcardrepayments,0)  sum_amount37_creditcardrepayments
,nvl(avg_amount37_creditcardrepayments,0)  avg_amount37_creditcardrepayments
,nvl(max_amount37_creditcardrepayments,0)  max_amount37_creditcardrepayments
,nvl(min_amount37_creditcardrepayments,0)  min_amount37_creditcardrepayments
,nvl(sum_cnt37_creditcardrepayments   ,0)  sum_cnt37_creditcardrepayments   
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount3_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount3_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount3_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount3_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt3_creditcardrepayments    
,nvl(sum_amount38_creditcardrepayments,0)  sum_amount38_creditcardrepayments
,nvl(avg_amount38_creditcardrepayments,0)  avg_amount38_creditcardrepayments
,nvl(max_amount38_creditcardrepayments,0)  max_amount38_creditcardrepayments
,nvl(min_amount38_creditcardrepayments,0)  min_amount38_creditcardrepayments
,nvl(sum_cnt38_creditcardrepayments   ,0)  sum_cnt38_creditcardrepayments   
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount2_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount2_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount2_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount2_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt2_creditcardrepayments    
,nvl(sum_amount6_creditcardrepayments ,0)  sum_amount1_creditcardrepayments 
,nvl(avg_amount6_creditcardrepayments ,0)  avg_amount1_creditcardrepayments 
,nvl(max_amount6_creditcardrepayments ,0)  max_amount1_creditcardrepayments 
,nvl(min_amount6_creditcardrepayments ,0)  min_amount1_creditcardrepayments 
,nvl(sum_cnt6_creditcardrepayments    ,0)  sum_cnt1_creditcardrepayments    
,nvl(sum_amount39_creditcardrepayments,0)  sum_amount39_creditcardrepayments
,nvl(avg_amount39_creditcardrepayments,0)  avg_amount39_creditcardrepayments
,nvl(max_amount39_creditcardrepayments,0)  max_amount39_creditcardrepayments
,nvl(min_amount39_creditcardrepayments,0)  min_amount39_creditcardrepayments
,nvl(sum_cnt39_creditcardrepayments   ,0)  sum_cnt39_creditcardrepayments   
,nvl(sum_amount40_creditcardrepayments,0)  sum_amount40_creditcardrepayments
,nvl(avg_amount40_creditcardrepayments,0)  avg_amount40_creditcardrepayments
,nvl(max_amount40_creditcardrepayments,0)  max_amount40_creditcardrepayments
,nvl(min_amount40_creditcardrepayments,0)  min_amount40_creditcardrepayments
,nvl(sum_cnt40_creditcardrepayments   ,0)  sum_cnt40_creditcardrepayments   
,nvl(sum_amount41_creditcardrepayments,0)  sum_amount41_creditcardrepayments
,nvl(avg_amount41_creditcardrepayments,0)  avg_amount41_creditcardrepayments
,nvl(max_amount41_creditcardrepayments,0)  max_amount41_creditcardrepayments
,nvl(min_amount41_creditcardrepayments,0)  min_amount41_creditcardrepayments
,nvl(sum_cnt41_creditcardrepayments   ,0)  sum_cnt41_creditcardrepayments   
,nvl(sum_amount42_creditcardrepayments,0)  sum_amount42_creditcardrepayments
,nvl(avg_amount42_creditcardrepayments,0)  avg_amount42_creditcardrepayments
,nvl(max_amount42_creditcardrepayments,0)  max_amount42_creditcardrepayments
,nvl(min_amount42_creditcardrepayments,0)  min_amount42_creditcardrepayments
,nvl(sum_cnt42_creditcardrepayments   ,0)  sum_cnt42_creditcardrepayments   
,nvl(sum_amount43_creditcardrepayments,0)  sum_amount43_creditcardrepayments
,nvl(avg_amount43_creditcardrepayments,0)  avg_amount43_creditcardrepayments
,nvl(max_amount43_creditcardrepayments,0)  max_amount43_creditcardrepayments
,nvl(min_amount43_creditcardrepayments,0)  min_amount43_creditcardrepayments
,nvl(sum_cnt43_creditcardrepayments   ,0)  sum_cnt43_creditcardrepayments   
,nvl(sum_amount44_creditcardrepayments,0)  sum_amount44_creditcardrepayments
,nvl(avg_amount44_creditcardrepayments,0)  avg_amount44_creditcardrepayments
,nvl(max_amount44_creditcardrepayments,0)  max_amount44_creditcardrepayments
,nvl(min_amount44_creditcardrepayments,0)  min_amount44_creditcardrepayments
,nvl(sum_cnt44_creditcardrepayments   ,0)  sum_cnt44_creditcardrepayments   
,nvl(sum_amount45_creditcardrepayments,0)  sum_amount45_creditcardrepayments
,nvl(avg_amount45_creditcardrepayments,0)  avg_amount45_creditcardrepayments
,nvl(max_amount45_creditcardrepayments,0)  max_amount45_creditcardrepayments
,nvl(min_amount45_creditcardrepayments,0)  min_amount45_creditcardrepayments
,nvl(sum_cnt45_creditcardrepayments   ,0)  sum_cnt45_creditcardrepayments   
,nvl(sum_amount46_creditcardrepayments,0)  sum_amount46_creditcardrepayments
,nvl(avg_amount46_creditcardrepayments,0)  avg_amount46_creditcardrepayments
,nvl(max_amount46_creditcardrepayments,0)  max_amount46_creditcardrepayments
,nvl(min_amount46_creditcardrepayments,0)  min_amount46_creditcardrepayments
,nvl(sum_cnt46_creditcardrepayments   ,0)  sum_cnt46_creditcardrepayments   
,nvl(sum_amount47_creditcardrepayments,0)  sum_amount47_creditcardrepayments
,nvl(avg_amount47_creditcardrepayments,0)  avg_amount47_creditcardrepayments
,nvl(max_amount47_creditcardrepayments,0)  max_amount47_creditcardrepayments
,nvl(min_amount47_creditcardrepayments,0)  min_amount47_creditcardrepayments
,nvl(sum_cnt47_creditcardrepayments   ,0)  sum_cnt47_creditcardrepayments   
,nvl(sum_amount48_creditcardrepayments,0)  sum_amount48_creditcardrepayments
,nvl(avg_amount48_creditcardrepayments,0)  avg_amount48_creditcardrepayments
,nvl(max_amount48_creditcardrepayments,0)  max_amount48_creditcardrepayments
,nvl(min_amount48_creditcardrepayments,0)  min_amount48_creditcardrepayments
,nvl(sum_cnt48_creditcardrepayments   ,0)  sum_cnt48_creditcardrepayments   
,nvl(sum_amount49_creditcardrepayments,0)  sum_amount49_creditcardrepayments
,nvl(avg_amount49_creditcardrepayments,0)  avg_amount49_creditcardrepayments
,nvl(max_amount49_creditcardrepayments,0)  max_amount49_creditcardrepayments
,nvl(min_amount49_creditcardrepayments,0)  min_amount49_creditcardrepayments
,nvl(sum_cnt49_creditcardrepayments   ,0)  sum_cnt49_creditcardrepayments   
,nvl(sum_amount50_creditcardrepayments,0)  sum_amount50_creditcardrepayments
,nvl(avg_amount50_creditcardrepayments,0)  avg_amount50_creditcardrepayments
,nvl(max_amount50_creditcardrepayments,0)  max_amount50_creditcardrepayments
,nvl(min_amount50_creditcardrepayments,0)  min_amount50_creditcardrepayments
,nvl(sum_cnt50_creditcardrepayments   ,0)  sum_cnt50_creditcardrepayments   
,nvl(sum_amount51_creditcardrepayments,0)  sum_amount51_creditcardrepayments
,nvl(avg_amount51_creditcardrepayments,0)  avg_amount51_creditcardrepayments
,nvl(max_amount51_creditcardrepayments,0)  max_amount51_creditcardrepayments
,nvl(min_amount51_creditcardrepayments,0)  min_amount51_creditcardrepayments
,nvl(sum_cnt51_creditcardrepayments   ,0)  sum_cnt51_creditcardrepayments   
,nvl(sum_amount52_creditcardrepayments,0)  sum_amount52_creditcardrepayments
,nvl(avg_amount52_creditcardrepayments,0)  avg_amount52_creditcardrepayments
,nvl(max_amount52_creditcardrepayments,0)  max_amount52_creditcardrepayments
,nvl(min_amount52_creditcardrepayments,0)  min_amount52_creditcardrepayments
,nvl(sum_cnt52_creditcardrepayments   ,0)  sum_cnt52_creditcardrepayments   
,nvl(sum_amount53_creditcardrepayments,0)  sum_amount53_creditcardrepayments
,nvl(avg_amount53_creditcardrepayments,0)  avg_amount53_creditcardrepayments
,nvl(max_amount53_creditcardrepayments,0)  max_amount53_creditcardrepayments
,nvl(min_amount53_creditcardrepayments,0)  min_amount53_creditcardrepayments
,nvl(sum_cnt53_creditcardrepayments   ,0)  sum_cnt53_creditcardrepayments   
,nvl(sum_amount54_creditcardrepayments,0)  sum_amount54_creditcardrepayments
,nvl(avg_amount54_creditcardrepayments,0)  avg_amount54_creditcardrepayments
,nvl(max_amount54_creditcardrepayments,0)  max_amount54_creditcardrepayments
,nvl(min_amount54_creditcardrepayments,0)  min_amount54_creditcardrepayments
,nvl(sum_cnt54_creditcardrepayments   ,0)  sum_cnt54_creditcardrepayments   
,nvl(sum_amount17_enquiryfee          ,0)  sum_amount17_enquiryfee          
,nvl(avg_amount17_enquiryfee          ,0)  avg_amount17_enquiryfee          
,nvl(max_amount17_enquiryfee          ,0)  max_amount17_enquiryfee          
,nvl(sum_cnt17_enquiryfee             ,0)  sum_cnt17_enquiryfee             
,nvl(min_amount17_enquiryfee          ,0)  min_amount17_enquiryfee          
,nvl(sum_amount16_enquiryfee          ,0)  sum_amount16_enquiryfee          
,nvl(avg_amount16_enquiryfee          ,0)  avg_amount16_enquiryfee          
,nvl(max_amount16_enquiryfee          ,0)  max_amount16_enquiryfee          
,nvl(sum_cnt16_enquiryfee             ,0)  sum_cnt16_enquiryfee             
,nvl(min_amount16_enquiryfee          ,0)  min_amount16_enquiryfee          
,nvl(sum_amount18_enquiryfee          ,0)  sum_amount18_enquiryfee          
,nvl(avg_amount18_enquiryfee          ,0)  avg_amount18_enquiryfee          
,nvl(max_amount18_enquiryfee          ,0)  max_amount18_enquiryfee          
,nvl(sum_cnt18_enquiryfee             ,0)  sum_cnt18_enquiryfee             
,nvl(min_amount18_enquiryfee          ,0)  min_amount18_enquiryfee          
,nvl(sum_amount15_enquiryfee          ,0)  sum_amount15_enquiryfee          
,nvl(avg_amount15_enquiryfee          ,0)  avg_amount15_enquiryfee          
,nvl(max_amount15_enquiryfee          ,0)  max_amount15_enquiryfee          
,nvl(sum_cnt15_enquiryfee             ,0)  sum_cnt15_enquiryfee             
,nvl(min_amount15_enquiryfee          ,0)  min_amount15_enquiryfee          
,nvl(sum_amount19_enquiryfee          ,0)  sum_amount19_enquiryfee          
,nvl(avg_amount19_enquiryfee          ,0)  avg_amount19_enquiryfee          
,nvl(max_amount19_enquiryfee          ,0)  max_amount19_enquiryfee          
,nvl(sum_cnt19_enquiryfee             ,0)  sum_cnt19_enquiryfee             
,nvl(min_amount19_enquiryfee          ,0)  min_amount19_enquiryfee          
,nvl(sum_amount20_enquiryfee          ,0)  sum_amount20_enquiryfee          
,nvl(avg_amount20_enquiryfee          ,0)  avg_amount20_enquiryfee          
,nvl(max_amount20_enquiryfee          ,0)  max_amount20_enquiryfee          
,nvl(sum_cnt20_enquiryfee             ,0)  sum_cnt20_enquiryfee             
,nvl(min_amount20_enquiryfee          ,0)  min_amount20_enquiryfee          
,nvl(sum_amount14_enquiryfee          ,0)  sum_amount14_enquiryfee          
,nvl(avg_amount14_enquiryfee          ,0)  avg_amount14_enquiryfee          
,nvl(max_amount14_enquiryfee          ,0)  max_amount14_enquiryfee          
,nvl(sum_cnt14_enquiryfee             ,0)  sum_cnt14_enquiryfee             
,nvl(min_amount14_enquiryfee          ,0)  min_amount14_enquiryfee          
,nvl(sum_amount21_enquiryfee          ,0)  sum_amount21_enquiryfee          
,nvl(avg_amount21_enquiryfee          ,0)  avg_amount21_enquiryfee          
,nvl(max_amount21_enquiryfee          ,0)  max_amount21_enquiryfee          
,nvl(sum_cnt21_enquiryfee             ,0)  sum_cnt21_enquiryfee             
,nvl(min_amount21_enquiryfee          ,0)  min_amount21_enquiryfee          
,nvl(sum_amount13_enquiryfee          ,0)  sum_amount13_enquiryfee          
,nvl(avg_amount13_enquiryfee          ,0)  avg_amount13_enquiryfee          
,nvl(max_amount13_enquiryfee          ,0)  max_amount13_enquiryfee          
,nvl(sum_cnt13_enquiryfee             ,0)  sum_cnt13_enquiryfee             
,nvl(min_amount13_enquiryfee          ,0)  min_amount13_enquiryfee          
,nvl(sum_amount22_enquiryfee          ,0)  sum_amount22_enquiryfee          
,nvl(avg_amount22_enquiryfee          ,0)  avg_amount22_enquiryfee          
,nvl(max_amount22_enquiryfee          ,0)  max_amount22_enquiryfee          
,nvl(sum_cnt22_enquiryfee             ,0)  sum_cnt22_enquiryfee             
,nvl(min_amount22_enquiryfee          ,0)  min_amount22_enquiryfee          
,nvl(sum_amount12_enquiryfee          ,0)  sum_amount12_enquiryfee          
,nvl(avg_amount12_enquiryfee          ,0)  avg_amount12_enquiryfee          
,nvl(max_amount12_enquiryfee          ,0)  max_amount12_enquiryfee          
,nvl(min_amount12_enquiryfee          ,0)  min_amount12_enquiryfee          
,nvl(sum_cnt12_enquiryfee             ,0)  sum_cnt12_enquiryfee             
,nvl(sum_amount23_enquiryfee          ,0)  sum_amount23_enquiryfee          
,nvl(avg_amount23_enquiryfee          ,0)  avg_amount23_enquiryfee          
,nvl(max_amount23_enquiryfee          ,0)  max_amount23_enquiryfee          
,nvl(sum_cnt23_enquiryfee             ,0)  sum_cnt23_enquiryfee             
,nvl(min_amount23_enquiryfee          ,0)  min_amount23_enquiryfee          
,nvl(sum_amount11_enquiryfee          ,0)  sum_amount11_enquiryfee          
,nvl(avg_amount11_enquiryfee          ,0)  avg_amount11_enquiryfee          
,nvl(max_amount11_enquiryfee          ,0)  max_amount11_enquiryfee          
,nvl(min_amount11_enquiryfee          ,0)  min_amount11_enquiryfee          
,nvl(sum_cnt11_enquiryfee             ,0)  sum_cnt11_enquiryfee             
,nvl(sum_amount24_enquiryfee          ,0)  sum_amount24_enquiryfee          
,nvl(avg_amount24_enquiryfee          ,0)  avg_amount24_enquiryfee          
,nvl(max_amount24_enquiryfee          ,0)  max_amount24_enquiryfee          
,nvl(sum_cnt24_enquiryfee             ,0)  sum_cnt24_enquiryfee             
,nvl(min_amount24_enquiryfee          ,0)  min_amount24_enquiryfee          
,nvl(sum_amount10_enquiryfee          ,0)  sum_amount10_enquiryfee          
,nvl(avg_amount10_enquiryfee          ,0)  avg_amount10_enquiryfee          
,nvl(max_amount10_enquiryfee          ,0)  max_amount10_enquiryfee          
,nvl(sum_cnt10_enquiryfee             ,0)  sum_cnt10_enquiryfee             
,nvl(min_amount10_enquiryfee          ,0)  min_amount10_enquiryfee          
,nvl(sum_amount9_enquiryfee           ,0)  sum_amount9_enquiryfee           
,nvl(avg_amount9_enquiryfee           ,0)  avg_amount9_enquiryfee           
,nvl(max_amount9_enquiryfee           ,0)  max_amount9_enquiryfee           
,nvl(sum_cnt9_enquiryfee              ,0)  sum_cnt9_enquiryfee              
,nvl(min_amount9_enquiryfee           ,0)  min_amount9_enquiryfee           
,nvl(sum_amount25_enquiryfee          ,0)  sum_amount25_enquiryfee          
,nvl(avg_amount25_enquiryfee          ,0)  avg_amount25_enquiryfee          
,nvl(max_amount25_enquiryfee          ,0)  max_amount25_enquiryfee          
,nvl(sum_cnt25_enquiryfee             ,0)  sum_cnt25_enquiryfee             
,nvl(min_amount25_enquiryfee          ,0)  min_amount25_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount8_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount8_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount8_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt8_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount8_enquiryfee           
,nvl(sum_amount26_enquiryfee          ,0)  sum_amount26_enquiryfee          
,nvl(avg_amount26_enquiryfee          ,0)  avg_amount26_enquiryfee          
,nvl(max_amount26_enquiryfee          ,0)  max_amount26_enquiryfee          
,nvl(sum_cnt26_enquiryfee             ,0)  sum_cnt26_enquiryfee             
,nvl(min_amount26_enquiryfee          ,0)  min_amount26_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount7_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount7_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount7_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt7_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount7_enquiryfee           
,nvl(sum_amount27_enquiryfee          ,0)  sum_amount27_enquiryfee          
,nvl(avg_amount27_enquiryfee          ,0)  avg_amount27_enquiryfee          
,nvl(max_amount27_enquiryfee          ,0)  max_amount27_enquiryfee          
,nvl(sum_cnt27_enquiryfee             ,0)  sum_cnt27_enquiryfee             
,nvl(min_amount27_enquiryfee          ,0)  min_amount27_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount6_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount6_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount6_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt6_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount6_enquiryfee           
,nvl(sum_amount60_creditcardrepayments,0)  sum_amount28_enquiryfee          
,nvl(avg_amount60_creditcardrepayments,0)  avg_amount28_enquiryfee          
,nvl(max_amount60_creditcardrepayments,0)  max_amount28_enquiryfee          
,nvl(min_amount60_creditcardrepayments,0)  sum_cnt28_enquiryfee             
,nvl(sum_cnt60_creditcardrepayments   ,0)  min_amount28_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount5_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount5_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount5_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt5_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount5_enquiryfee           
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount4_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount4_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount4_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt4_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount4_enquiryfee           
,nvl(sum_amount29_enquiryfee          ,0)  sum_amount29_enquiryfee          
,nvl(avg_amount29_enquiryfee          ,0)  avg_amount29_enquiryfee          
,nvl(max_amount29_enquiryfee          ,0)  max_amount29_enquiryfee          
,nvl(sum_cnt29_enquiryfee             ,0)  sum_cnt29_enquiryfee             
,nvl(min_amount29_enquiryfee          ,0)  min_amount29_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount3_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount3_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount3_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  min_amount3_enquiryfee           
,nvl(min_amount8_enquiryfee           ,0)  sum_cnt3_enquiryfee              
,nvl(sum_amount30_enquiryfee          ,0)  sum_amount30_enquiryfee          
,nvl(avg_amount30_enquiryfee          ,0)  avg_amount30_enquiryfee          
,nvl(max_amount30_enquiryfee          ,0)  max_amount30_enquiryfee          
,nvl(sum_cnt30_enquiryfee             ,0)  sum_cnt30_enquiryfee             
,nvl(min_amount30_enquiryfee          ,0)  min_amount30_enquiryfee          
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount2_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount2_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount2_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt2_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount2_enquiryfee           
,nvl(sum_amount8_enquiryfee           ,0)  sum_amount1_enquiryfee           
,nvl(avg_amount8_enquiryfee           ,0)  avg_amount1_enquiryfee           
,nvl(max_amount8_enquiryfee           ,0)  max_amount1_enquiryfee           
,nvl(sum_cnt8_enquiryfee              ,0)  sum_cnt1_enquiryfee              
,nvl(min_amount8_enquiryfee           ,0)  min_amount1_enquiryfee           
,nvl(sum_amount31_enquiryfee          ,0)  sum_amount31_enquiryfee          
,nvl(avg_amount31_enquiryfee          ,0)  avg_amount31_enquiryfee          
,nvl(max_amount31_enquiryfee          ,0)  max_amount31_enquiryfee          
,nvl(sum_cnt31_enquiryfee             ,0)  sum_cnt31_enquiryfee             
,nvl(min_amount31_enquiryfee          ,0)  min_amount31_enquiryfee          
,nvl(sum_amount32_enquiryfee          ,0)  sum_amount32_enquiryfee          
,nvl(avg_amount32_enquiryfee          ,0)  avg_amount32_enquiryfee          
,nvl(max_amount32_enquiryfee          ,0)  max_amount32_enquiryfee          
,nvl(sum_cnt32_enquiryfee             ,0)  sum_cnt32_enquiryfee             
,nvl(min_amount32_enquiryfee          ,0)  min_amount32_enquiryfee          
,nvl(sum_amount33_enquiryfee          ,0)  sum_amount33_enquiryfee          
,nvl(avg_amount33_enquiryfee          ,0)  avg_amount33_enquiryfee          
,nvl(max_amount33_enquiryfee          ,0)  max_amount33_enquiryfee          
,nvl(sum_cnt33_enquiryfee             ,0)  sum_cnt33_enquiryfee             
,nvl(min_amount33_enquiryfee          ,0)  min_amount33_enquiryfee          
,nvl(sum_amount34_enquiryfee          ,0)  sum_amount34_enquiryfee          
,nvl(avg_amount34_enquiryfee          ,0)  avg_amount34_enquiryfee          
,nvl(max_amount34_enquiryfee          ,0)  max_amount34_enquiryfee          
,nvl(sum_cnt34_enquiryfee             ,0)  sum_cnt34_enquiryfee             
,nvl(min_amount34_enquiryfee          ,0)  min_amount34_enquiryfee          
,nvl(sum_amount35_enquiryfee          ,0)  sum_amount35_enquiryfee          
,nvl(avg_amount35_enquiryfee          ,0)  avg_amount35_enquiryfee          
,nvl(max_amount35_enquiryfee          ,0)  max_amount35_enquiryfee          
,nvl(sum_cnt35_enquiryfee             ,0)  sum_cnt35_enquiryfee             
,nvl(min_amount35_enquiryfee          ,0)  min_amount35_enquiryfee          
,nvl(sum_amount36_enquiryfee          ,0)  sum_amount36_enquiryfee          
,nvl(avg_amount36_enquiryfee          ,0)  avg_amount36_enquiryfee          
,nvl(max_amount36_enquiryfee          ,0)  max_amount36_enquiryfee          
,nvl(sum_cnt36_enquiryfee             ,0)  sum_cnt36_enquiryfee             
,nvl(min_amount36_enquiryfee          ,0)  min_amount36_enquiryfee          
,nvl(sum_amount37_enquiryfee          ,0)  sum_amount37_enquiryfee          
,nvl(avg_amount37_enquiryfee          ,0)  avg_amount37_enquiryfee          
,nvl(max_amount37_enquiryfee          ,0)  max_amount37_enquiryfee          
,nvl(sum_cnt37_enquiryfee             ,0)  sum_cnt37_enquiryfee             
,nvl(min_amount37_enquiryfee          ,0)  min_amount37_enquiryfee          
,nvl(sum_amount38_enquiryfee          ,0)  sum_amount38_enquiryfee          
,nvl(avg_amount38_enquiryfee          ,0)  avg_amount38_enquiryfee          
,nvl(max_amount38_enquiryfee          ,0)  max_amount38_enquiryfee          
,nvl(sum_cnt38_enquiryfee             ,0)  sum_cnt38_enquiryfee             
,nvl(min_amount38_enquiryfee          ,0)  min_amount38_enquiryfee          
,nvl(sum_amount39_enquiryfee          ,0)  sum_amount39_enquiryfee          
,nvl(avg_amount39_enquiryfee          ,0)  avg_amount39_enquiryfee          
,nvl(max_amount39_enquiryfee          ,0)  max_amount39_enquiryfee          
,nvl(sum_cnt39_enquiryfee             ,0)  sum_cnt39_enquiryfee             
,nvl(min_amount39_enquiryfee          ,0)  min_amount39_enquiryfee          
,nvl(sum_amount40_enquiryfee          ,0)  sum_amount40_enquiryfee          
,nvl(avg_amount40_enquiryfee          ,0)  avg_amount40_enquiryfee          
,nvl(max_amount40_enquiryfee          ,0)  max_amount40_enquiryfee          
,nvl(min_amount40_enquiryfee          ,0)  min_amount40_enquiryfee          
,nvl(sum_cnt40_enquiryfee             ,0)  sum_cnt40_enquiryfee             
,nvl(sum_amount41_enquiryfee          ,0)  sum_amount41_enquiryfee          
,nvl(avg_amount41_enquiryfee          ,0)  avg_amount41_enquiryfee          
,nvl(max_amount41_enquiryfee          ,0)  max_amount41_enquiryfee          
,nvl(sum_cnt41_enquiryfee             ,0)  sum_cnt41_enquiryfee             
,nvl(min_amount41_enquiryfee          ,0)  min_amount41_enquiryfee          
,nvl(sum_amount42_enquiryfee          ,0)  sum_amount42_enquiryfee          
,nvl(avg_amount42_enquiryfee          ,0)  avg_amount42_enquiryfee          
,nvl(max_amount42_enquiryfee          ,0)  max_amount42_enquiryfee          
,nvl(sum_cnt42_enquiryfee             ,0)  sum_cnt42_enquiryfee             
,nvl(min_amount42_enquiryfee          ,0)  min_amount42_enquiryfee          

from phone_variable_yfq_tnh_2
;


drop table  phone_variable_tnh_creditcardrepayments_train_tc_result;
create table phone_variable_tnh_creditcardrepayments_train_tc_result   
as select
label
,phone
,case when sum_cnt60_creditcardrepayments            >5.0     then 5.0      else          sum_cnt60_creditcardrepayments             end   sum_cnt60_creditcardrepayments   
,case when sum_cnt59_creditcardrepayments            >5.0     then 5.0      else          sum_cnt59_creditcardrepayments             end   sum_cnt59_creditcardrepayments   
,case when sum_cnt58_creditcardrepayments            >6.0     then 6.0      else          sum_cnt58_creditcardrepayments             end   sum_cnt58_creditcardrepayments   
,case when sum_cnt57_creditcardrepayments            >6.0     then 6.0      else          sum_cnt57_creditcardrepayments             end   sum_cnt57_creditcardrepayments   
,case when sum_cnt56_creditcardrepayments            >6.0     then 6.0      else          sum_cnt56_creditcardrepayments             end   sum_cnt56_creditcardrepayments   
,case when sum_cnt55_creditcardrepayments            >6.0     then 6.0      else          sum_cnt55_creditcardrepayments             end   sum_cnt55_creditcardrepayments   
,case when sum_cnt54_creditcardrepayments            >7.0     then 7.0      else          sum_cnt54_creditcardrepayments             end   sum_cnt54_creditcardrepayments   
,case when sum_cnt53_creditcardrepayments            >7.0     then 7.0      else          sum_cnt53_creditcardrepayments             end   sum_cnt53_creditcardrepayments   
,case when sum_cnt52_creditcardrepayments            >7.0     then 7.0       else         sum_cnt52_creditcardrepayments             end   sum_cnt52_creditcardrepayments   
,case when sum_cnt51_creditcardrepayments            >8.0     then 8.0       else         sum_cnt51_creditcardrepayments             end   sum_cnt51_creditcardrepayments   
,case when sum_cnt50_creditcardrepayments            >7.0     then 7.0       else         sum_cnt50_creditcardrepayments             end   sum_cnt50_creditcardrepayments   
,case when sum_cnt49_creditcardrepayments            >8.0     then 8.0       else         sum_cnt49_creditcardrepayments             end   sum_cnt49_creditcardrepayments   
,case when sum_cnt48_creditcardrepayments            >7.0     then 7.0       else         sum_cnt48_creditcardrepayments             end   sum_cnt48_creditcardrepayments   
,case when sum_cnt47_creditcardrepayments            >8.0     then 8.0       else         sum_cnt47_creditcardrepayments             end   sum_cnt47_creditcardrepayments   
,case when sum_cnt46_creditcardrepayments            >8.0     then 8.0       else         sum_cnt46_creditcardrepayments             end   sum_cnt46_creditcardrepayments   
,case when sum_cnt45_creditcardrepayments            >9.0     then 9.0       else         sum_cnt45_creditcardrepayments             end   sum_cnt45_creditcardrepayments   
,case when sum_cnt44_creditcardrepayments            >8.0     then 8.0       else         sum_cnt44_creditcardrepayments             end   sum_cnt44_creditcardrepayments   
,case when sum_cnt43_creditcardrepayments            >9.0     then 9.0       else         sum_cnt43_creditcardrepayments             end   sum_cnt43_creditcardrepayments   
,case when sum_cnt42_creditcardrepayments            >9.0     then 9.0       else         sum_cnt42_creditcardrepayments             end   sum_cnt42_creditcardrepayments   
,case when sum_cnt41_creditcardrepayments            >10.0    then 10.0      else         sum_cnt41_creditcardrepayments             end   sum_cnt41_creditcardrepayments   
,case when sum_cnt40_creditcardrepayments            >8.0     then 8.0       else         sum_cnt40_creditcardrepayments             end   sum_cnt40_creditcardrepayments   
,case when sum_cnt39_creditcardrepayments            >10.0    then 10.0      else         sum_cnt39_creditcardrepayments             end   sum_cnt39_creditcardrepayments   
,case when sum_cnt38_creditcardrepayments            >9.0     then 9.0       else         sum_cnt38_creditcardrepayments             end   sum_cnt38_creditcardrepayments   
,case when sum_cnt37_creditcardrepayments            >10.0    then 10.0      else         sum_cnt37_creditcardrepayments             end   sum_cnt37_creditcardrepayments   
,case when sum_cnt36_creditcardrepayments            >10.0    then 10.0      else         sum_cnt36_creditcardrepayments             end   sum_cnt36_creditcardrepayments   
,case when sum_cnt35_creditcardrepayments            >11.0    then 11.0      else         sum_cnt35_creditcardrepayments             end   sum_cnt35_creditcardrepayments   
,case when sum_cnt34_creditcardrepayments            >11.0    then 11.0      else         sum_cnt34_creditcardrepayments             end   sum_cnt34_creditcardrepayments   
,case when sum_cnt33_creditcardrepayments            >11.0    then 11.0      else         sum_cnt33_creditcardrepayments             end   sum_cnt33_creditcardrepayments   
,case when sum_cnt32_creditcardrepayments            >11.0    then 11.0      else         sum_cnt32_creditcardrepayments             end   sum_cnt32_creditcardrepayments   
,case when sum_cnt31_creditcardrepayments            >11.0    then 11.0      else         sum_cnt31_creditcardrepayments             end   sum_cnt31_creditcardrepayments   
,case when sum_cnt30_creditcardrepayments            >11.0    then 11.0      else         sum_cnt30_creditcardrepayments             end   sum_cnt30_creditcardrepayments   
,case when sum_cnt29_creditcardrepayments            >11.0    then 11.0      else         sum_cnt29_creditcardrepayments             end   sum_cnt29_creditcardrepayments   
,case when sum_cnt28_creditcardrepayments            >9.0     then 9.0       else         sum_cnt28_creditcardrepayments             end   sum_cnt28_creditcardrepayments   
,case when sum_cnt27_creditcardrepayments            >11.0    then 11.0      else         sum_cnt27_creditcardrepayments             end   sum_cnt27_creditcardrepayments   
,case when sum_cnt26_creditcardrepayments            >10.0    then 10.0      else         sum_cnt26_creditcardrepayments             end   sum_cnt26_creditcardrepayments   
,case when sum_cnt25_creditcardrepayments            >9.0     then 9.0       else         sum_cnt25_creditcardrepayments             end   sum_cnt25_creditcardrepayments   
,case when sum_cnt24_creditcardrepayments            >9.0     then 9.0       else         sum_cnt24_creditcardrepayments             end   sum_cnt24_creditcardrepayments   
,case when sum_cnt23_creditcardrepayments            >9.0     then 9.0       else         sum_cnt23_creditcardrepayments             end   sum_cnt23_creditcardrepayments   
,case when sum_cnt22_creditcardrepayments            >8.0     then 8.0       else         sum_cnt22_creditcardrepayments             end   sum_cnt22_creditcardrepayments   
,case when sum_cnt21_creditcardrepayments            >7.0     then 7.0       else         sum_cnt21_creditcardrepayments             end   sum_cnt21_creditcardrepayments   
,case when sum_cnt20_creditcardrepayments            >7.0     then 7.0       else         sum_cnt20_creditcardrepayments             end   sum_cnt20_creditcardrepayments   
,case when sum_cnt19_creditcardrepayments            >6.0     then 6.0       else         sum_cnt19_creditcardrepayments             end   sum_cnt19_creditcardrepayments   
,case when sum_cnt18_creditcardrepayments            >6.0     then 6.0       else         sum_cnt18_creditcardrepayments             end   sum_cnt18_creditcardrepayments   
,case when sum_cnt17_creditcardrepayments            >6.0     then 6.0       else         sum_cnt17_creditcardrepayments             end   sum_cnt17_creditcardrepayments   
,case when sum_amount60_creditcardrepayments         >16020.0 then 16020.0   else         sum_amount60_creditcardrepayments          end   sum_amount60_creditcardrepayments
,case when sum_amount59_creditcardrepayments         >18865.0 then 18865.0   else         sum_amount59_creditcardrepayments          end   sum_amount59_creditcardrepayments
,case when sum_amount58_creditcardrepayments         >22135.0 then 22135.0   else         sum_amount58_creditcardrepayments          end   sum_amount58_creditcardrepayments
,case when sum_amount57_creditcardrepayments         >23700.  then 23700.    else         sum_amount57_creditcardrepayments          end   sum_amount57_creditcardrepayments
,case when sum_amount56_creditcardrepayments         >24107.0 then 24107.0   else         sum_amount56_creditcardrepayments          end   sum_amount56_creditcardrepayments
,case when sum_amount55_creditcardrepayments         >28874.0 then 28874.0   else         sum_amount55_creditcardrepayments          end   sum_amount55_creditcardrepayments
,case when sum_amount54_creditcardrepayments         >33403.0 then 33403.0   else         sum_amount54_creditcardrepayments          end   sum_amount54_creditcardrepayments
,case when sum_amount53_creditcardrepayments         >37720.0 then 37720.0   else         sum_amount53_creditcardrepayments          end   sum_amount53_creditcardrepayments
,case when sum_amount52_creditcardrepayments         >32858.0 then 32858.0   else         sum_amount52_creditcardrepayments          end   sum_amount52_creditcardrepayments
,case when sum_amount51_creditcardrepayments         >39329.0 then 39329.0   else         sum_amount51_creditcardrepayments          end   sum_amount51_creditcardrepayments
,case when sum_amount50_creditcardrepayments         >37940.0 then 37940.0   else         sum_amount50_creditcardrepayments          end   sum_amount50_creditcardrepayments
,case when sum_amount49_creditcardrepayments         >42736.0 then 42736.0   else         sum_amount49_creditcardrepayments          end   sum_amount49_creditcardrepayments
,case when sum_amount48_creditcardrepayments         >40272.0 then 40272.0   else         sum_amount48_creditcardrepayments          end   sum_amount48_creditcardrepayments
,case when sum_amount47_creditcardrepayments         >46794.0 then 46794.0   else         sum_amount47_creditcardrepayments          end   sum_amount47_creditcardrepayments
,case when sum_amount46_creditcardrepayments         >49082.0 then 49082.0   else         sum_amount46_creditcardrepayments          end   sum_amount46_creditcardrepayments
,case when sum_amount45_creditcardrepayments         >52980.0 then 52980.0   else         sum_amount45_creditcardrepayments          end   sum_amount45_creditcardrepayments
,case when sum_amount44_creditcardrepayments         >54876.0 then 54876.0   else         sum_amount44_creditcardrepayments          end   sum_amount44_creditcardrepayments
,case when sum_amount43_creditcardrepayments         >53800.0 then 53800.0   else         sum_amount43_creditcardrepayments          end   sum_amount43_creditcardrepayments
,case when sum_amount42_creditcardrepayments         >61712.0 then 61712.0   else         sum_amount42_creditcardrepayments          end   sum_amount42_creditcardrepayments
,case when sum_amount41_creditcardrepayments         >59960.0 then 59960.0   else         sum_amount41_creditcardrepayments          end   sum_amount41_creditcardrepayments
,case when sum_amount40_creditcardrepayments         >51536.0 then 51536.0   else         sum_amount40_creditcardrepayments          end   sum_amount40_creditcardrepayments
,case when sum_amount39_creditcardrepayments         >64332.0 then 64332.0   else         sum_amount39_creditcardrepayments          end   sum_amount39_creditcardrepayments
,case when sum_amount38_creditcardrepayments         >60962.0 then 60962.0   else         sum_amount38_creditcardrepayments          end   sum_amount38_creditcardrepayments
,case when sum_amount37_creditcardrepayments         >64268.0 then 64268.0   else         sum_amount37_creditcardrepayments          end   sum_amount37_creditcardrepayments
,case when sum_amount36_creditcardrepayments         >65936.0 then 65936.0   else         sum_amount36_creditcardrepayments          end   sum_amount36_creditcardrepayments
,case when sum_amount35_creditcardrepayments         >70931.0 then 70931.0   else         sum_amount35_creditcardrepayments          end   sum_amount35_creditcardrepayments
,case when sum_amount34_creditcardrepayments         >72034.0 then 72034.0   else         sum_amount34_creditcardrepayments          end   sum_amount34_creditcardrepayments
,case when sum_amount33_creditcardrepayments         >76602.  then 76602.    else         sum_amount33_creditcardrepayments          end   sum_amount33_creditcardrepayments
,case when sum_amount32_creditcardrepayments         >71744.0 then 71744.0   else         sum_amount32_creditcardrepayments          end   sum_amount32_creditcardrepayments
,case when sum_amount31_creditcardrepayments         >72802.0 then 72802.0   else         sum_amount31_creditcardrepayments          end   sum_amount31_creditcardrepayments
,case when sum_amount30_creditcardrepayments         >76741.0 then 76741.0   else         sum_amount30_creditcardrepayments          end   sum_amount30_creditcardrepayments
,case when sum_amount29_creditcardrepayments         >70852.0 then 70852.0   else         sum_amount29_creditcardrepayments          end   sum_amount29_creditcardrepayments
,case when sum_amount28_creditcardrepayments         >58702.0 then 58702.0   else         sum_amount28_creditcardrepayments          end   sum_amount28_creditcardrepayments
,case when sum_amount27_creditcardrepayments         >70995.0 then 70995.0   else         sum_amount27_creditcardrepayments          end   sum_amount27_creditcardrepayments
,case when sum_amount26_creditcardrepayments         >64420.0 then 64420.0   else         sum_amount26_creditcardrepayments          end   sum_amount26_creditcardrepayments
,case when sum_amount25_creditcardrepayments         >58983.0 then 58983.0   else         sum_amount25_creditcardrepayments          end   sum_amount25_creditcardrepayments
,case when sum_amount24_creditcardrepayments         >57322.0 then 57322.0   else         sum_amount24_creditcardrepayments          end   sum_amount24_creditcardrepayments
,case when sum_amount23_creditcardrepayments         >50910.0 then 50910.0   else         sum_amount23_creditcardrepayments          end   sum_amount23_creditcardrepayments
,case when sum_amount22_creditcardrepayments         >48923.0 then 48923.0   else         sum_amount22_creditcardrepayments          end   sum_amount22_creditcardrepayments
,case when sum_amount21_creditcardrepayments         >45718.0 then 45718.0   else         sum_amount21_creditcardrepayments          end   sum_amount21_creditcardrepayments
,case when sum_amount20_creditcardrepayments         >41568.0 then 41568.0   else         sum_amount20_creditcardrepayments          end   sum_amount20_creditcardrepayments
,case when sum_amount19_creditcardrepayments         >36409.0 then 36409.0   else         sum_amount19_creditcardrepayments          end   sum_amount19_creditcardrepayments
,case when sum_amount18_creditcardrepayments         >33932.0 then 33932.0   else         sum_amount18_creditcardrepayments          end   sum_amount18_creditcardrepayments
,case when sum_amount17_creditcardrepayments         >31886.0 then 31886.0   else         sum_amount17_creditcardrepayments          end   sum_amount17_creditcardrepayments
,case when min_amount60_creditcardrepayments         >3294.0  then 3294.0    else         min_amount60_creditcardrepayments          end   min_amount60_creditcardrepayments
,case when min_amount59_creditcardrepayments         >3681.0  then 3681.0    else         min_amount59_creditcardrepayments          end   min_amount59_creditcardrepayments
,case when min_amount58_creditcardrepayments         >3792.0  then 3792.0    else         min_amount58_creditcardrepayments          end   min_amount58_creditcardrepayments
,case when min_amount57_creditcardrepayments         >3969.0  then 3969.0    else         min_amount57_creditcardrepayments          end   min_amount57_creditcardrepayments
,case when min_amount56_creditcardrepayments         >4167.0  then 4167.0    else         min_amount56_creditcardrepayments          end   min_amount56_creditcardrepayments
,case when min_amount55_creditcardrepayments         >4999.0  then 4999.0    else         min_amount55_creditcardrepayments          end   min_amount55_creditcardrepayments
,case when min_amount54_creditcardrepayments         >5136.0  then 5136.0    else         min_amount54_creditcardrepayments          end   min_amount54_creditcardrepayments
,case when min_amount53_creditcardrepayments         >5535.0  then 5535.0    else         min_amount53_creditcardrepayments          end   min_amount53_creditcardrepayments
,case when min_amount52_creditcardrepayments         >5385.0  then 5385.0    else         min_amount52_creditcardrepayments          end   min_amount52_creditcardrepayments
,case when min_amount51_creditcardrepayments         >5989.0  then 5989.0    else         min_amount51_creditcardrepayments          end   min_amount51_creditcardrepayments
,case when min_amount50_creditcardrepayments         >6781.0  then 6781.0     else        min_amount50_creditcardrepayments          end   min_amount50_creditcardrepayments
,case when min_amount49_creditcardrepayments         >7393.0  then 7393.0     else        min_amount49_creditcardrepayments          end   min_amount49_creditcardrepayments
,case when min_amount48_creditcardrepayments         >8007.0  then 8007.0     else        min_amount48_creditcardrepayments          end   min_amount48_creditcardrepayments
,case when min_amount47_creditcardrepayments         >8320.0  then 8320.0     else        min_amount47_creditcardrepayments          end   min_amount47_creditcardrepayments
,case when min_amount46_creditcardrepayments         >8574.0  then 8574.0     else        min_amount46_creditcardrepayments          end   min_amount46_creditcardrepayments
,case when min_amount45_creditcardrepayments         >9568.0  then 9568.0     else        min_amount45_creditcardrepayments          end   min_amount45_creditcardrepayments
,case when min_amount44_creditcardrepayments         >9946.0  then 9946.0     else        min_amount44_creditcardrepayments          end   min_amount44_creditcardrepayments
,case when min_amount43_creditcardrepayments         >9966.0  then 9966.0   else          min_amount43_creditcardrepayments          end   min_amount43_creditcardrepayments
,case when min_amount42_creditcardrepayments         >9212.0  then 9212.0   else          min_amount42_creditcardrepayments          end   min_amount42_creditcardrepayments
,case when min_amount41_creditcardrepayments         >9985.0  then 9985.0   else          min_amount41_creditcardrepayments          end   min_amount41_creditcardrepayments
,case when min_amount40_creditcardrepayments         >9950.0  then 9950.0   else          min_amount40_creditcardrepayments          end   min_amount40_creditcardrepayments
,case when min_amount39_creditcardrepayments         >10000.0 then 10000.0  else          min_amount39_creditcardrepayments          end   min_amount39_creditcardrepayments
,case when min_amount38_creditcardrepayments         >9977.0  then 9977.0   else          min_amount38_creditcardrepayments          end   min_amount38_creditcardrepayments
,case when min_amount37_creditcardrepayments         >9964.0  then 9964.0   else          min_amount37_creditcardrepayments          end   min_amount37_creditcardrepayments
,case when min_amount36_creditcardrepayments         >9998.0  then 9998.0   else          min_amount36_creditcardrepayments          end   min_amount36_creditcardrepayments
,case when min_amount35_creditcardrepayments         >10742.0 then 10742.0   else         min_amount35_creditcardrepayments          end   min_amount35_creditcardrepayments
,case when min_amount34_creditcardrepayments         >10257.0 then 10257.0   else         min_amount34_creditcardrepayments          end   min_amount34_creditcardrepayments
,case when min_amount33_creditcardrepayments         >9997.0  then 9997.0    else         min_amount33_creditcardrepayments          end   min_amount33_creditcardrepayments
,case when min_amount32_creditcardrepayments         >10000.0 then 10000.0   else         min_amount32_creditcardrepayments          end   min_amount32_creditcardrepayments
,case when min_amount31_creditcardrepayments         >10698.0 then 10698.0   else         min_amount31_creditcardrepayments          end   min_amount31_creditcardrepayments
,case when min_amount30_creditcardrepayments         >10000.0 then 10000.0   else         min_amount30_creditcardrepayments          end   min_amount30_creditcardrepayments
,case when min_amount29_creditcardrepayments         >10000.0 then 10000.0   else         min_amount29_creditcardrepayments          end   min_amount29_creditcardrepayments
,case when min_amount28_creditcardrepayments         >10000.0 then 10000.0   else         min_amount28_creditcardrepayments          end   min_amount28_creditcardrepayments
,case when min_amount27_creditcardrepayments         >9999.0  then 9999.0    else         min_amount27_creditcardrepayments          end   min_amount27_creditcardrepayments
,case when min_amount26_creditcardrepayments         >9956.0  then 9956.0    else         min_amount26_creditcardrepayments          end   min_amount26_creditcardrepayments
,case when min_amount25_creditcardrepayments         >10000.0 then 10000.0   else         min_amount25_creditcardrepayments          end   min_amount25_creditcardrepayments
,case when min_amount24_creditcardrepayments         >9929.0  then 9929.0    else         min_amount24_creditcardrepayments          end   min_amount24_creditcardrepayments
,case when min_amount23_creditcardrepayments         >8210.0  then 8210.0    else         min_amount23_creditcardrepayments          end   min_amount23_creditcardrepayments
,case when min_amount22_creditcardrepayments         >7989.0  then 7989.0    else         min_amount22_creditcardrepayments          end   min_amount22_creditcardrepayments
,case when min_amount21_creditcardrepayments         >7189.0  then 7189.0    else         min_amount21_creditcardrepayments          end   min_amount21_creditcardrepayments
,case when min_amount20_creditcardrepayments         >6827.0  then 6827.0    else         min_amount20_creditcardrepayments          end   min_amount20_creditcardrepayments
,case when min_amount19_creditcardrepayments         >6364.0  then 6364.0    else         min_amount19_creditcardrepayments          end   min_amount19_creditcardrepayments
,case when min_amount18_creditcardrepayments         >5376.0  then 5376.0    else         min_amount18_creditcardrepayments          end   min_amount18_creditcardrepayments
,case when min_amount17_creditcardrepayments         >5000.0  then 5000.0    else         min_amount17_creditcardrepayments          end   min_amount17_creditcardrepayments
,case when max_amount60_creditcardrepayments         >9600.0  then 9600.0    else         max_amount60_creditcardrepayments          end   max_amount60_creditcardrepayments
,case when max_amount59_creditcardrepayments         >10292.0 then 10292.0   else         max_amount59_creditcardrepayments          end   max_amount59_creditcardrepayments
,case when max_amount58_creditcardrepayments         >12996.0 then 12996.0   else         max_amount58_creditcardrepayments          end   max_amount58_creditcardrepayments
,case when max_amount57_creditcardrepayments         >13352.0 then 13352.0   else         max_amount57_creditcardrepayments          end   max_amount57_creditcardrepayments
,case when max_amount56_creditcardrepayments         >13972.0 then 13972.0   else         max_amount56_creditcardrepayments          end   max_amount56_creditcardrepayments
,case when max_amount55_creditcardrepayments         >15995.0 then 15995.0   else         max_amount55_creditcardrepayments          end   max_amount55_creditcardrepayments
,case when max_amount54_creditcardrepayments         >17991.0 then 17991.0   else         max_amount54_creditcardrepayments          end   max_amount54_creditcardrepayments
,case when max_amount53_creditcardrepayments         >19451.0 then 19451.0   else         max_amount53_creditcardrepayments          end   max_amount53_creditcardrepayments
,case when max_amount52_creditcardrepayments         >17768.0 then 17768.0   else         max_amount52_creditcardrepayments          end   max_amount52_creditcardrepayments
,case when max_amount51_creditcardrepayments         >20697.0 then 20697.0   else         max_amount51_creditcardrepayments          end   max_amount51_creditcardrepayments
,case when max_amount50_creditcardrepayments         >20000.0 then 20000.0   else         max_amount50_creditcardrepayments          end   max_amount50_creditcardrepayments
,case when max_amount49_creditcardrepayments         >20947.  then 20947.    else         max_amount49_creditcardrepayments          end   max_amount49_creditcardrepayments
,case when max_amount48_creditcardrepayments         >21752.0 then 21752.0   else         max_amount48_creditcardrepayments          end   max_amount48_creditcardrepayments
,case when max_amount47_creditcardrepayments         >24453.0 then 24453.0   else         max_amount47_creditcardrepayments          end   max_amount47_creditcardrepayments
,case when max_amount46_creditcardrepayments         >24564.0 then 24564.0   else         max_amount46_creditcardrepayments          end   max_amount46_creditcardrepayments
,case when max_amount45_creditcardrepayments         >26529.0 then 26529.0   else         max_amount45_creditcardrepayments          end   max_amount45_creditcardrepayments
,case when max_amount44_creditcardrepayments         >26267.0 then 26267.0   else         max_amount44_creditcardrepayments          end   max_amount44_creditcardrepayments
,case when max_amount43_creditcardrepayments         >25423.0 then 25423.0   else         max_amount43_creditcardrepayments          end   max_amount43_creditcardrepayments
,case when max_amount42_creditcardrepayments         >25168.0 then 25168.0   else         max_amount42_creditcardrepayments          end   max_amount42_creditcardrepayments
,case when max_amount41_creditcardrepayments         >25924.0 then 25924.0   else         max_amount41_creditcardrepayments          end   max_amount41_creditcardrepayments
,case when max_amount40_creditcardrepayments         >23967.0 then 23967.0   else         max_amount40_creditcardrepayments          end   max_amount40_creditcardrepayments
,case when max_amount39_creditcardrepayments         >25000.0 then 25000.0   else         max_amount39_creditcardrepayments          end   max_amount39_creditcardrepayments
,case when max_amount38_creditcardrepayments         >24963.0 then 24963.0   else         max_amount38_creditcardrepayments          end   max_amount38_creditcardrepayments
,case when max_amount37_creditcardrepayments         >26432.0 then 26432.0   else         max_amount37_creditcardrepayments          end   max_amount37_creditcardrepayments
,case when max_amount36_creditcardrepayments         >26344.0 then 26344.0   else         max_amount36_creditcardrepayments          end   max_amount36_creditcardrepayments
,case when max_amount35_creditcardrepayments         >27434.0 then 27434.0   else         max_amount35_creditcardrepayments          end   max_amount35_creditcardrepayments
,case when max_amount34_creditcardrepayments         >26958.0 then 26958.0   else         max_amount34_creditcardrepayments          end   max_amount34_creditcardrepayments
,case when max_amount33_creditcardrepayments         >29842.0 then 29842.0   else         max_amount33_creditcardrepayments          end   max_amount33_creditcardrepayments
,case when max_amount32_creditcardrepayments         >28197.0 then 28197.0   else         max_amount32_creditcardrepayments          end   max_amount32_creditcardrepayments
,case when max_amount31_creditcardrepayments         >27611.0 then 27611.0   else         max_amount31_creditcardrepayments          end   max_amount31_creditcardrepayments
,case when max_amount30_creditcardrepayments         >28018.0 then 28018.0   else         max_amount30_creditcardrepayments          end   max_amount30_creditcardrepayments
,case when max_amount29_creditcardrepayments         >26553.0 then 26553.0   else         max_amount29_creditcardrepayments          end   max_amount29_creditcardrepayments
,case when max_amount28_creditcardrepayments         >24377.0 then 24377.0   else         max_amount28_creditcardrepayments          end   max_amount28_creditcardrepayments
,case when max_amount27_creditcardrepayments         >26593.0 then 26593.0   else         max_amount27_creditcardrepayments          end   max_amount27_creditcardrepayments
,case when max_amount26_creditcardrepayments         >24151.0 then 24151.0   else         max_amount26_creditcardrepayments          end   max_amount26_creditcardrepayments
,case when max_amount25_creditcardrepayments         >21566.0 then 21566.0   else         max_amount25_creditcardrepayments          end   max_amount25_creditcardrepayments
,case when max_amount24_creditcardrepayments         >22943.0 then 22943.0   else         max_amount24_creditcardrepayments          end   max_amount24_creditcardrepayments
,case when max_amount23_creditcardrepayments         >20998.0 then 20998.0   else         max_amount23_creditcardrepayments          end   max_amount23_creditcardrepayments
,case when max_amount22_creditcardrepayments         >19951.0 then 19951.0   else         max_amount22_creditcardrepayments          end   max_amount22_creditcardrepayments
,case when max_amount21_creditcardrepayments         >18401.0 then 18401.0   else         max_amount21_creditcardrepayments          end   max_amount21_creditcardrepayments
,case when max_amount20_creditcardrepayments         >17468.0 then 17468.0   else         max_amount20_creditcardrepayments          end   max_amount20_creditcardrepayments
,case when max_amount19_creditcardrepayments         >15107.0 then 15107.0   else         max_amount19_creditcardrepayments          end   max_amount19_creditcardrepayments
,case when max_amount18_creditcardrepayments         >15098.0 then 15098.0   else         max_amount18_creditcardrepayments          end   max_amount18_creditcardrepayments
,case when max_amount17_creditcardrepayments         >14999.0 then 14999.0   else         max_amount17_creditcardrepayments          end   max_amount17_creditcardrepayments
,case when avg_amount60_creditcardrepayments         >5496.0  then 5496.0    else         avg_amount60_creditcardrepayments          end   avg_amount60_creditcardrepayments
,case when avg_amount59_creditcardrepayments         >6202.0  then 6202.0    else         avg_amount59_creditcardrepayments          end   avg_amount59_creditcardrepayments
,case when avg_amount58_creditcardrepayments         >7498.0  then 7498.0    else         avg_amount58_creditcardrepayments          end   avg_amount58_creditcardrepayments
,case when avg_amount57_creditcardrepayments         >7342.0  then 7342.0    else         avg_amount57_creditcardrepayments          end   avg_amount57_creditcardrepayments
,case when avg_amount56_creditcardrepayments         >7839.0  then 7839.0    else         avg_amount56_creditcardrepayments          end   avg_amount56_creditcardrepayments
,case when avg_amount55_creditcardrepayments         >8686.0  then 8686.0    else         avg_amount55_creditcardrepayments          end   avg_amount55_creditcardrepayments
,case when avg_amount54_creditcardrepayments         >9928.0  then 9928.0    else         avg_amount54_creditcardrepayments          end   avg_amount54_creditcardrepayments
,case when avg_amount53_creditcardrepayments         >10006.0 then 10006.0   else         avg_amount53_creditcardrepayments          end   avg_amount53_creditcardrepayments
,case when avg_amount52_creditcardrepayments         >9174.0  then 9174.0    else         avg_amount52_creditcardrepayments          end   avg_amount52_creditcardrepayments
,case when avg_amount51_creditcardrepayments         >11000.0 then 11000.0   else         avg_amount51_creditcardrepayments          end   avg_amount51_creditcardrepayments
,case when avg_amount50_creditcardrepayments         >11192.0 then 11192.0   else         avg_amount50_creditcardrepayments          end   avg_amount50_creditcardrepayments
,case when avg_amount49_creditcardrepayments         >12367.0 then 12367.0   else         avg_amount49_creditcardrepayments          end   avg_amount49_creditcardrepayments
,case when avg_amount48_creditcardrepayments         >12451.0 then 12451.0   else         avg_amount48_creditcardrepayments          end   avg_amount48_creditcardrepayments
,case when avg_amount47_creditcardrepayments         >12572.0 then 12572.0   else         avg_amount47_creditcardrepayments          end   avg_amount47_creditcardrepayments
,case when avg_amount46_creditcardrepayments         >13068.0 then 13068.0   else         avg_amount46_creditcardrepayments          end   avg_amount46_creditcardrepayments
,case when avg_amount45_creditcardrepayments         >14805.  then 14805.    else         avg_amount45_creditcardrepayments          end   avg_amount45_creditcardrepayments
,case when avg_amount44_creditcardrepayments         >14192.0 then 14192.0   else         avg_amount44_creditcardrepayments          end   avg_amount44_creditcardrepayments
,case when avg_amount43_creditcardrepayments         >14672.0 then 14672.0   else         avg_amount43_creditcardrepayments          end   avg_amount43_creditcardrepayments
,case when avg_amount42_creditcardrepayments         >14115.0 then 14115.0   else         avg_amount42_creditcardrepayments          end   avg_amount42_creditcardrepayments
,case when avg_amount41_creditcardrepayments         >14426.0 then 14426.0   else         avg_amount41_creditcardrepayments          end   avg_amount41_creditcardrepayments
,case when avg_amount40_creditcardrepayments         >13848.0 then 13848.0   else         avg_amount40_creditcardrepayments          end   avg_amount40_creditcardrepayments
,case when avg_amount39_creditcardrepayments         >14814.0 then 14814.0   else         avg_amount39_creditcardrepayments          end   avg_amount39_creditcardrepayments
,case when avg_amount38_creditcardrepayments         >14123.0 then 14123.0   else         avg_amount38_creditcardrepayments          end   avg_amount38_creditcardrepayments
,case when avg_amount37_creditcardrepayments         >14675.0 then 14675.0   else         avg_amount37_creditcardrepayments          end   avg_amount37_creditcardrepayments
,case when avg_amount36_creditcardrepayments         >14467.0 then 14467.0   else         avg_amount36_creditcardrepayments          end   avg_amount36_creditcardrepayments
,case when avg_amount35_creditcardrepayments         >15000.0 then 15000.0   else         avg_amount35_creditcardrepayments          end   avg_amount35_creditcardrepayments
,case when avg_amount34_creditcardrepayments         >14990.0 then 14990.0   else         avg_amount34_creditcardrepayments          end   avg_amount34_creditcardrepayments
,case when avg_amount33_creditcardrepayments         >15430.0 then 15430.0    else        avg_amount33_creditcardrepayments          end   avg_amount33_creditcardrepayments
,case when avg_amount32_creditcardrepayments         >14999.0 then 14999.0    else        avg_amount32_creditcardrepayments          end   avg_amount32_creditcardrepayments
,case when avg_amount31_creditcardrepayments         >15162.0 then 15162.0    else        avg_amount31_creditcardrepayments          end   avg_amount31_creditcardrepayments
,case when avg_amount30_creditcardrepayments         >14484.0 then 14484.0    else        avg_amount30_creditcardrepayments          end   avg_amount30_creditcardrepayments
,case when avg_amount29_creditcardrepayments         >14998.0 then 14998.0    else        avg_amount29_creditcardrepayments          end   avg_amount29_creditcardrepayments
,case when avg_amount28_creditcardrepayments         >13982.0 then 13982.0    else        avg_amount28_creditcardrepayments          end   avg_amount28_creditcardrepayments
,case when avg_amount27_creditcardrepayments         >14496.0 then 14496.0    else        avg_amount27_creditcardrepayments          end   avg_amount27_creditcardrepayments
,case when avg_amount26_creditcardrepayments         >13376.0 then 13376.0  else          avg_amount26_creditcardrepayments          end   avg_amount26_creditcardrepayments
,case when avg_amount25_creditcardrepayments         >13502.0 then 13502.0  else          avg_amount25_creditcardrepayments          end   avg_amount25_creditcardrepayments
,case when avg_amount24_creditcardrepayments         >13935.0 then 13935.0  else          avg_amount24_creditcardrepayments          end   avg_amount24_creditcardrepayments
,case when avg_amount23_creditcardrepayments         >12247.0 then 12247.0  else          avg_amount23_creditcardrepayments          end   avg_amount23_creditcardrepayments
,case when avg_amount22_creditcardrepayments         >11983.0 then 11983.0  else          avg_amount22_creditcardrepayments          end   avg_amount22_creditcardrepayments
,case when avg_amount21_creditcardrepayments         >10952.  then 10952.   else          avg_amount21_creditcardrepayments          end   avg_amount21_creditcardrepayments
,case when avg_amount20_creditcardrepayments         >10407.0 then 10407.0  else          avg_amount20_creditcardrepayments          end   avg_amount20_creditcardrepayments
,case when avg_amount19_creditcardrepayments         >9998.0  then 9998.0   else          avg_amount19_creditcardrepayments          end   avg_amount19_creditcardrepayments
,case when avg_amount18_creditcardrepayments         >9424.0  then 9424.0    else         avg_amount18_creditcardrepayments          end   avg_amount18_creditcardrepayments
,case when avg_amount17_creditcardrepayments         >9171.0  then 9171.0    else         avg_amount17_creditcardrepayments          end   avg_amount17_creditcardrepayments
,case when min_amount22_enquiryfee                   >7993.0  then 7993.0    else         min_amount22_enquiryfee                    end   min_amount22_enquiryfee          
,case when min_amount23_enquiryfee                   >7998.0  then 7998.0    else         min_amount23_enquiryfee                    end   min_amount23_enquiryfee          
,case when min_amount24_enquiryfee                   >8998.0  then 8998.0    else         min_amount24_enquiryfee                    end   min_amount24_enquiryfee          
,case when min_amount25_enquiryfee                   >9747.0  then 9747.0    else         min_amount25_enquiryfee                    end   min_amount25_enquiryfee          
,case when min_amount26_enquiryfee                   >8999.0  then 8999.0    else         min_amount26_enquiryfee                    end   min_amount26_enquiryfee          
,case when min_amount27_enquiryfee                   >9584.0  then 9584.0    else         min_amount27_enquiryfee                    end   min_amount27_enquiryfee          
,case when min_amount28_enquiryfee                   >9993.0  then 9993.0    else         min_amount28_enquiryfee                    end   min_amount28_enquiryfee          
,case when min_amount29_enquiryfee                   >9999.0  then 9999.0    else         min_amount29_enquiryfee                    end   min_amount29_enquiryfee          
,case when min_amount30_enquiryfee                   >9999.0  then 9999.0    else         min_amount30_enquiryfee                    end   min_amount30_enquiryfee          
,case when min_amount31_enquiryfee                   >9992.0  then 9992.0    else         min_amount31_enquiryfee                    end   min_amount31_enquiryfee          
,case when min_amount32_enquiryfee                   >10000.0 then 10000.0   else         min_amount32_enquiryfee                    end   min_amount32_enquiryfee          
,case when min_amount33_enquiryfee                   >9999.0  then 9999.0    else         min_amount33_enquiryfee                    end   min_amount33_enquiryfee          
,case when min_amount34_enquiryfee                   >9999.0  then 9999.0    else         min_amount34_enquiryfee                    end   min_amount34_enquiryfee          
,case when min_amount35_enquiryfee                   >9983.0  then 9983.0    else         min_amount35_enquiryfee                    end   min_amount35_enquiryfee          
,case when min_amount36_enquiryfee                   >9474.0  then 9474.0    else         min_amount36_enquiryfee                    end   min_amount36_enquiryfee          
,case when min_amount37_enquiryfee                   >9008.0  then 9008.0    else         min_amount37_enquiryfee                    end   min_amount37_enquiryfee          
,case when min_amount38_enquiryfee                   >8197.0  then 8197.0    else         min_amount38_enquiryfee                    end   min_amount38_enquiryfee          
,case when min_amount39_enquiryfee                   >9207.0  then 9207.0    else         min_amount39_enquiryfee                    end   min_amount39_enquiryfee          
,case when min_amount40_enquiryfee                   >7936.0  then 7936.0    else         min_amount40_enquiryfee                    end   min_amount40_enquiryfee          
,case when min_amount41_enquiryfee                   >7571.0  then 7571.0    else         min_amount41_enquiryfee                    end   min_amount41_enquiryfee          
,case when min_amount42_enquiryfee                   >7430.0  then 7430.0    else         min_amount42_enquiryfee                    end   min_amount42_enquiryfee          
,case when min_amount43_enquiryfee                   >7932.0  then 7932.0    else         min_amount43_enquiryfee                    end   min_amount43_enquiryfee          
,case when min_amount44_enquiryfee                   >7482.0  then 7482.0    else         min_amount44_enquiryfee                    end   min_amount44_enquiryfee          
,case when min_amount45_enquiryfee                   >7909.0  then 7909.0    else         min_amount45_enquiryfee                    end   min_amount45_enquiryfee          
,case when min_amount46_enquiryfee                   >6842.0  then 6842.0    else         min_amount46_enquiryfee                    end   min_amount46_enquiryfee          
,case when min_amount47_enquiryfee                   >5841.0  then 5841.0    else         min_amount47_enquiryfee                    end   min_amount47_enquiryfee          
,case when min_amount48_enquiryfee                   >5007.0  then 5007.0    else         min_amount48_enquiryfee                    end   min_amount48_enquiryfee          
,case when min_amount49_enquiryfee                   >3993.0  then 3993.0    else         min_amount49_enquiryfee                    end   min_amount49_enquiryfee          
,case when min_amount50_enquiryfee                   >3915.0  then 3915.0    else         min_amount50_enquiryfee                    end   min_amount50_enquiryfee          
,case when sum_amount18_enquiryfee                   >43074.0 then 43074.0   else         sum_amount18_enquiryfee                    end   sum_amount18_enquiryfee          
,case when sum_amount19_enquiryfee                   >42762.0 then 42762.0   else         sum_amount19_enquiryfee                    end   sum_amount19_enquiryfee          
,case when sum_amount20_enquiryfee                   >52122.0 then 52122.0   else         sum_amount20_enquiryfee                    end   sum_amount20_enquiryfee          
,case when sum_amount21_enquiryfee                   >58575.0 then 58575.0   else         sum_amount21_enquiryfee                    end   sum_amount21_enquiryfee          
,case when sum_amount22_enquiryfee                   >63368.0 then 63368.0   else         sum_amount22_enquiryfee                    end   sum_amount22_enquiryfee          
,case when sum_amount23_enquiryfee                   >72290.0 then 72290.0   else         sum_amount23_enquiryfee                    end   sum_amount23_enquiryfee          
,case when sum_amount24_enquiryfee                   >68340.0 then 68340.0   else         sum_amount24_enquiryfee                    end   sum_amount24_enquiryfee          
,case when sum_amount25_enquiryfee                   >63890.0 then 63890.0   else         sum_amount25_enquiryfee                    end   sum_amount25_enquiryfee          
,case when sum_amount26_enquiryfee                   >71160.0 then 71160.0   else         sum_amount26_enquiryfee                    end   sum_amount26_enquiryfee          
,case when sum_amount27_enquiryfee                   >75077.0 then 75077.0   else         sum_amount27_enquiryfee                    end   sum_amount27_enquiryfee          
,case when sum_amount28_enquiryfee                   >64378.0 then 64378.0   else         sum_amount28_enquiryfee                    end   sum_amount28_enquiryfee          
,case when sum_amount29_enquiryfee                   >84142.0 then 84142.0   else         sum_amount29_enquiryfee                    end   sum_amount29_enquiryfee          
,case when sum_amount30_enquiryfee                   >81123.0 then 81123.0   else         sum_amount30_enquiryfee                    end   sum_amount30_enquiryfee          
,case when sum_amount31_enquiryfee                   >75096.0 then 75096.0   else         sum_amount31_enquiryfee                    end   sum_amount31_enquiryfee          
,case when sum_amount32_enquiryfee                   >72472.  then 72472.    else         sum_amount32_enquiryfee                    end   sum_amount32_enquiryfee          
,case when sum_amount33_enquiryfee                   >84833.0 then 84833.0   else         sum_amount33_enquiryfee                    end   sum_amount33_enquiryfee          
,case when sum_amount34_enquiryfee                   >72380.0 then 72380.0   else         sum_amount34_enquiryfee                    end   sum_amount34_enquiryfee          
,case when sum_amount35_enquiryfee                   >75981.0 then 75981.0   else         sum_amount35_enquiryfee                    end   sum_amount35_enquiryfee          
,case when sum_amount36_enquiryfee                   >70597.0 then 70597.0   else         sum_amount36_enquiryfee                    end   sum_amount36_enquiryfee          
,case when sum_amount37_enquiryfee                   >70725.0 then 70725.0   else         sum_amount37_enquiryfee                    end   sum_amount37_enquiryfee          
,case when sum_amount38_enquiryfee                   >61868.0 then 61868.0   else         sum_amount38_enquiryfee                    end   sum_amount38_enquiryfee          
,case when sum_amount39_enquiryfee                   >62968.0 then 62968.0   else         sum_amount39_enquiryfee                    end   sum_amount39_enquiryfee          
,case when sum_amount40_enquiryfee                   >50098.0 then 50098.0   else         sum_amount40_enquiryfee                    end   sum_amount40_enquiryfee          
,case when sum_amount41_enquiryfee                   >65936.0 then 65936.0   else         sum_amount41_enquiryfee                    end   sum_amount41_enquiryfee          
,case when sum_amount42_enquiryfee                   >72685.0 then 72685.0   else         sum_amount42_enquiryfee                    end   sum_amount42_enquiryfee          
,case when sum_amount43_enquiryfee                   >55087.0 then 55087.0   else         sum_amount43_enquiryfee                    end   sum_amount43_enquiryfee          
,case when sum_amount44_enquiryfee                   >52340.0 then 52340.0   else         sum_amount44_enquiryfee                    end   sum_amount44_enquiryfee          
,case when sum_amount45_enquiryfee                   >48335.0 then 48335.0   else         sum_amount45_enquiryfee                    end   sum_amount45_enquiryfee          
,case when sum_amount46_enquiryfee                   >41683.0 then 41683.0   else         sum_amount46_enquiryfee                    end   sum_amount46_enquiryfee          
,case when sum_amount47_enquiryfee                   >36248.0 then 36248.0   else         sum_amount47_enquiryfee                    end   sum_amount47_enquiryfee          
,case when sum_amount48_enquiryfee                   >31175.0 then 31175.0   else         sum_amount48_enquiryfee                    end   sum_amount48_enquiryfee          
,case when sum_amount49_enquiryfee                   >29768.0 then 29768.0   else         sum_amount49_enquiryfee                    end   sum_amount49_enquiryfee          
,case when sum_amount50_enquiryfee                   >20174.0 then 20174.0   else         sum_amount50_enquiryfee                    end   sum_amount50_enquiryfee          
,case when sum_cnt18_enquiryfee                      >8.0     then 8.0       else         sum_cnt18_enquiryfee                       end   sum_cnt18_enquiryfee             
,case when sum_cnt19_enquiryfee                      >9.0     then 9.0       else         sum_cnt19_enquiryfee                       end   sum_cnt19_enquiryfee             
,case when sum_cnt20_enquiryfee                      >10.0    then 10.0      else         sum_cnt20_enquiryfee                       end   sum_cnt20_enquiryfee             
,case when sum_cnt21_enquiryfee                      >11.0    then 11.0      else         sum_cnt21_enquiryfee                       end   sum_cnt21_enquiryfee             
,case when sum_cnt22_enquiryfee                      >12.0    then 12.0      else         sum_cnt22_enquiryfee                       end   sum_cnt22_enquiryfee             
,case when sum_cnt23_enquiryfee                      >13.0    then 13.0      else         sum_cnt23_enquiryfee                       end   sum_cnt23_enquiryfee             
,case when sum_cnt24_enquiryfee                      >11.0    then 11.0      else         sum_cnt24_enquiryfee                       end   sum_cnt24_enquiryfee             
,case when sum_cnt25_enquiryfee                      >11.0    then 11.0      else         sum_cnt25_enquiryfee                       end   sum_cnt25_enquiryfee             
,case when sum_cnt26_enquiryfee                      >11.0    then 11.0      else         sum_cnt26_enquiryfee                       end   sum_cnt26_enquiryfee             
,case when sum_cnt27_enquiryfee                      >13.0    then 13.0      else         sum_cnt27_enquiryfee                       end   sum_cnt27_enquiryfee             
,case when sum_cnt28_enquiryfee                      >10.0    then 10.0      else         sum_cnt28_enquiryfee                       end   sum_cnt28_enquiryfee             
,case when sum_cnt29_enquiryfee                      >12.0    then 12.0      else         sum_cnt29_enquiryfee                       end   sum_cnt29_enquiryfee             
,case when sum_cnt30_enquiryfee                      >12.0    then 12.0      else         sum_cnt30_enquiryfee                       end   sum_cnt30_enquiryfee             
,case when sum_cnt31_enquiryfee                      >12.0    then 12.0      else         sum_cnt31_enquiryfee                       end   sum_cnt31_enquiryfee             
,case when sum_cnt32_enquiryfee                      >12.0    then 12.0      else         sum_cnt32_enquiryfee                       end   sum_cnt32_enquiryfee             
,case when sum_cnt33_enquiryfee                      >12.0    then 12.0      else         sum_cnt33_enquiryfee                       end   sum_cnt33_enquiryfee             
,case when sum_cnt34_enquiryfee                      >11.0    then 11.0      else         sum_cnt34_enquiryfee                       end   sum_cnt34_enquiryfee             
,case when sum_cnt35_enquiryfee                      >12.0    then 12.0      else         sum_cnt35_enquiryfee                       end   sum_cnt35_enquiryfee             
,case when sum_cnt36_enquiryfee                      >10.0    then 10.0      else         sum_cnt36_enquiryfee                       end   sum_cnt36_enquiryfee             
,case when sum_cnt37_enquiryfee                      >11.0    then 11.0      else         sum_cnt37_enquiryfee                       end   sum_cnt37_enquiryfee             
,case when sum_cnt38_enquiryfee                      >10.0    then 10.0      else         sum_cnt38_enquiryfee                       end   sum_cnt38_enquiryfee             
,case when sum_cnt39_enquiryfee                      >10.0    then 10.0      else         sum_cnt39_enquiryfee                       end   sum_cnt39_enquiryfee             
,case when sum_cnt40_enquiryfee                      >9.0     then 9.0       else         sum_cnt40_enquiryfee                       end   sum_cnt40_enquiryfee             
,case when sum_cnt41_enquiryfee                      >12.0    then 12.0      else         sum_cnt41_enquiryfee                       end   sum_cnt41_enquiryfee             
,case when sum_cnt42_enquiryfee                      >12.0    then 12.0      else         sum_cnt42_enquiryfee                       end   sum_cnt42_enquiryfee             
,case when sum_cnt43_enquiryfee                      >10.0    then 10.0      else         sum_cnt43_enquiryfee                       end   sum_cnt43_enquiryfee             
,case when sum_cnt44_enquiryfee                      >10.0    then 10.0       else        sum_cnt44_enquiryfee                       end   sum_cnt44_enquiryfee             
,case when sum_cnt45_enquiryfee                      >9.0     then 9.0        else        sum_cnt45_enquiryfee                       end   sum_cnt45_enquiryfee             
,case when sum_cnt46_enquiryfee                      >8.0     then 8.0        else        sum_cnt46_enquiryfee                       end   sum_cnt46_enquiryfee             
,case when sum_cnt47_enquiryfee                      >8.0     then 8.0        else        sum_cnt47_enquiryfee                       end   sum_cnt47_enquiryfee             
,case when sum_cnt48_enquiryfee                      >7.0     then 7.0        else        sum_cnt48_enquiryfee                       end   sum_cnt48_enquiryfee             
,case when sum_cnt49_enquiryfee                      >7.0     then 7.0        else        sum_cnt49_enquiryfee                       end   sum_cnt49_enquiryfee             
,case when sum_cnt50_enquiryfee                      >6.0     then 6.0        else        sum_cnt50_enquiryfee                       end   sum_cnt50_enquiryfee             

from phone_variable_tnh_creditcardrepayments_train_tc;       

drop table  phone_variable_yfq_creditcardrepayments_train_tc_result;
create table phone_variable_yfq_creditcardrepayments_train_tc_result   
as select
label
,phone
,case when sum_amount30_enquiryfee             >348831.0 then 348831.0   else          sum_amount30_enquiryfee                end   sum_amount30_enquiryfee            
,case when avg_amount30_enquiryfee             >28105.0  then 28105.0    else          avg_amount30_enquiryfee                end   avg_amount30_enquiryfee             
,case when max_amount30_enquiryfee             >53811.0  then 53811.0    else          max_amount30_enquiryfee                end   max_amount30_enquiryfee             
,case when min_amount30_enquiryfee             >19799.0  then 19799.0    else          min_amount30_enquiryfee                end   min_amount30_enquiryfee             
,case when sum_cnt30_enquiryfee                >28.0     then 28.0       else          sum_cnt30_enquiryfee                   end   sum_cnt30_enquiryfee                
,case when sum_amount29_enquiryfee             >328061.0 then 328061.0   else          sum_amount29_enquiryfee                end   sum_amount29_enquiryfee             
,case when avg_amount29_enquiryfee             >27648.0  then 27648.0    else          avg_amount29_enquiryfee                end   avg_amount29_enquiryfee             
,case when max_amount29_enquiryfee             >50164.0  then 50164.0    else          max_amount29_enquiryfee                end   max_amount29_enquiryfee             
,case when sum_cnt29_enquiryfee                >27.0     then 27.0        else         sum_cnt29_enquiryfee                   end   sum_cnt29_enquiryfee                
,case when min_amount29_enquiryfee             >19000.0  then 19000.0     else         min_amount29_enquiryfee                end   min_amount29_enquiryfee             
,case when sum_amount33_enquiryfee             >317620.0 then 317620.0    else         sum_amount33_enquiryfee                end   sum_amount33_enquiryfee             
,case when avg_amount33_enquiryfee             >27432.0  then 27432.0     else         avg_amount33_enquiryfee                end   avg_amount33_enquiryfee             
,case when max_amount33_enquiryfee             >52384.0  then 52384.0     else         max_amount33_enquiryfee                end   max_amount33_enquiryfee             
,case when sum_cnt33_enquiryfee                >27.0     then 27.0        else         sum_cnt33_enquiryfee                   end   sum_cnt33_enquiryfee                
,case when min_amount33_enquiryfee             >18678.0  then 18678.0     else         min_amount33_enquiryfee                end   min_amount33_enquiryfee             
,case when sum_amount32_enquiryfee             >304998.0 then 304998.0    else         sum_amount32_enquiryfee                end   sum_amount32_enquiryfee             
,case when avg_amount32_enquiryfee             >27348.0  then 27348.0     else         avg_amount32_enquiryfee                end   avg_amount32_enquiryfee             
,case when max_amount32_enquiryfee             >51273.0  then 51273.0     else         max_amount32_enquiryfee                end   max_amount32_enquiryfee             
,case when sum_cnt32_enquiryfee                >25.0     then 25.0        else         sum_cnt32_enquiryfee                   end   sum_cnt32_enquiryfee                
,case when min_amount32_enquiryfee             >18998.   then 18998.      else         min_amount32_enquiryfee                end   min_amount32_enquiryfee             
,case when sum_amount31_enquiryfee             >316656.0 then 316656.0    else         sum_amount31_enquiryfee                end   sum_amount31_enquiryfee             
,case when avg_amount31_enquiryfee             >28114.0  then 28114.0     else         avg_amount31_enquiryfee                end   avg_amount31_enquiryfee             
,case when max_amount31_enquiryfee             >51741.0  then 51741.0     else         max_amount31_enquiryfee                end   max_amount31_enquiryfee             
,case when sum_cnt31_enquiryfee                >25.0     then 25.0        else         sum_cnt31_enquiryfee                   end   sum_cnt31_enquiryfee                
,case when min_amount31_enquiryfee             >19496.0  then 19496.0     else         min_amount31_enquiryfee                end   min_amount31_enquiryfee             
,case when sum_amount27_enquiryfee             >353467.0 then 353467.0    else         sum_amount27_enquiryfee                end   sum_amount27_enquiryfee             
,case when avg_amount27_enquiryfee             >27285.0  then 27285.0     else         avg_amount27_enquiryfee                end   avg_amount27_enquiryfee             
,case when max_amount27_enquiryfee             >50000.0  then 50000.0     else         max_amount27_enquiryfee                end   max_amount27_enquiryfee             
,case when min_amount27_enquiryfee             >18983.0  then 18983.0     else         min_amount27_enquiryfee                end   min_amount27_enquiryfee             
,case when sum_cnt27_enquiryfee                >29.0     then 29.0        else         sum_cnt27_enquiryfee                   end   sum_cnt27_enquiryfee                
,case when sum_amount34_enquiryfee             >295388.0 then 295388.0    else         sum_amount34_enquiryfee                end   sum_amount34_enquiryfee             
,case when avg_amount34_enquiryfee             >26499.0  then 26499.0     else         avg_amount34_enquiryfee                end   avg_amount34_enquiryfee             
,case when max_amount34_enquiryfee             >50000.0  then 50000.0     else         max_amount34_enquiryfee                end   max_amount34_enquiryfee             
,case when sum_cnt34_enquiryfee                >24.0     then 24.0        else         sum_cnt34_enquiryfee                   end   sum_cnt34_enquiryfee                
,case when min_amount34_enquiryfee             >16970.0  then 16970.0     else         min_amount34_enquiryfee                end   min_amount34_enquiryfee             
,case when sum_amount35_enquiryfee             >320055.0 then 320055.0    else         sum_amount35_enquiryfee                end   sum_amount35_enquiryfee             
,case when avg_amount35_enquiryfee             >26992.0  then 26992.0     else         avg_amount35_enquiryfee                end   avg_amount35_enquiryfee             
,case when max_amount35_enquiryfee             >51872.0  then 51872.0     else         max_amount35_enquiryfee                end   max_amount35_enquiryfee             
,case when sum_cnt35_enquiryfee                >26.0     then 26.0        else         sum_cnt35_enquiryfee                   end   sum_cnt35_enquiryfee                
,case when min_amount35_enquiryfee             >16982.0  then 16982.0     else         min_amount35_enquiryfee                end   min_amount35_enquiryfee             
,case when sum_amount36_enquiryfee             >294065.0 then 294065.0    else         sum_amount36_enquiryfee                end   sum_amount36_enquiryfee             
,case when avg_amount36_enquiryfee             >26315.0  then 26315.0     else         avg_amount36_enquiryfee                end   avg_amount36_enquiryfee             
,case when max_amount36_enquiryfee             >50000.0  then 50000.0     else         max_amount36_enquiryfee                end   max_amount36_enquiryfee             
,case when sum_cnt36_enquiryfee                >24.0     then 24.0        else         sum_cnt36_enquiryfee                   end   sum_cnt36_enquiryfee                
,case when min_amount36_enquiryfee             >15982.0  then 15982.0     else         min_amount36_enquiryfee                end   min_amount36_enquiryfee             
,case when sum_amount37_enquiryfee             >298012.0 then 298012.0    else         sum_amount37_enquiryfee                end   sum_amount37_enquiryfee             
,case when avg_amount37_enquiryfee             >26194.0  then 26194.0     else         avg_amount37_enquiryfee                end   avg_amount37_enquiryfee             
,case when max_amount37_enquiryfee             >50081.0  then 50081.0     else         max_amount37_enquiryfee                end   max_amount37_enquiryfee             
,case when sum_cnt37_enquiryfee                >25.0     then 25.0        else         sum_cnt37_enquiryfee                   end   sum_cnt37_enquiryfee                
,case when min_amount37_enquiryfee             >15993.0  then 15993.0     else         min_amount37_enquiryfee                end   min_amount37_enquiryfee             
,case when sum_amount28_enquiryfee             >277813.0 then 277813.0    else         sum_amount28_enquiryfee                end   sum_amount28_enquiryfee             
,case when avg_amount28_enquiryfee             >26595.0  then 26595.0     else         avg_amount28_enquiryfee                end   avg_amount28_enquiryfee             
,case when max_amount28_enquiryfee             >50000.0  then 50000.0     else         max_amount28_enquiryfee                end   max_amount28_enquiryfee             
,case when sum_cnt28_enquiryfee                >23.0     then 23.0        else         sum_cnt28_enquiryfee                   end   sum_cnt28_enquiryfee                
,case when min_amount28_enquiryfee             >18071.0  then 18071.0     else         min_amount28_enquiryfee                end   min_amount28_enquiryfee             
,case when sum_amount26_enquiryfee             >315383.0 then 315383.0    else         sum_amount26_enquiryfee                end   sum_amount26_enquiryfee             
,case when avg_amount26_enquiryfee             >27359.0  then 27359.0     else         avg_amount26_enquiryfee                end   avg_amount26_enquiryfee             
,case when max_amount26_enquiryfee             >50000.0  then 50000.0     else         max_amount26_enquiryfee                end   max_amount26_enquiryfee             
,case when sum_cnt26_enquiryfee                >26.0     then 26.0        else         sum_cnt26_enquiryfee                   end   sum_cnt26_enquiryfee                
,case when min_amount26_enquiryfee             >18000.0  then 18000.0     else         min_amount26_enquiryfee                end   min_amount26_enquiryfee             
,case when sum_amount25_enquiryfee             >314755.0 then 314755.0    else         sum_amount25_enquiryfee                end   sum_amount25_enquiryfee             
,case when avg_amount25_enquiryfee             >26672.0  then 26672.0     else         avg_amount25_enquiryfee                end   avg_amount25_enquiryfee             
,case when max_amount25_enquiryfee             >50000.0  then 50000.0     else         max_amount25_enquiryfee                end   max_amount25_enquiryfee             
,case when sum_cnt25_enquiryfee                >26.0     then 26.0        else         sum_cnt25_enquiryfee                   end   sum_cnt25_enquiryfee                
,case when min_amount25_enquiryfee             >17517.0  then 17517.0     else         min_amount25_enquiryfee                end   min_amount25_enquiryfee             
,case when sum_amount38_enquiryfee             >296964.0 then 296964.0    else         sum_amount38_enquiryfee                end   sum_amount38_enquiryfee             
,case when avg_amount38_enquiryfee             >25996.0  then 25996.0     else         avg_amount38_enquiryfee                end   avg_amount38_enquiryfee             
,case when max_amount38_enquiryfee             >50077.0  then 50077.0     else         max_amount38_enquiryfee                end   max_amount38_enquiryfee             
,case when sum_cnt38_enquiryfee                >24.0     then 24.0        else         sum_cnt38_enquiryfee                   end   sum_cnt38_enquiryfee                
,case when min_amount38_enquiryfee             >15790.0  then 15790.0     else         min_amount38_enquiryfee                end   min_amount38_enquiryfee             
,case when sum_amount24_enquiryfee             >316207.0 then 316207.0    else         sum_amount24_enquiryfee                end   sum_amount24_enquiryfee             
,case when avg_amount24_enquiryfee             >26627.0  then 26627.0     else         avg_amount24_enquiryfee                end   avg_amount24_enquiryfee             
,case when max_amount24_enquiryfee             >50000.0  then 50000.0     else         max_amount24_enquiryfee                end   max_amount24_enquiryfee             
,case when sum_cnt24_enquiryfee                >27.0     then 27.0        else         sum_cnt24_enquiryfee                   end   sum_cnt24_enquiryfee                
,case when min_amount24_enquiryfee             >17988.0  then 17988.0     else         min_amount24_enquiryfee                end   min_amount24_enquiryfee             
,case when sum_amount39_enquiryfee             >314533.0 then 314533.0    else         sum_amount39_enquiryfee                end   sum_amount39_enquiryfee             
,case when avg_amount39_enquiryfee             >25209.0  then 25209.0     else         avg_amount39_enquiryfee                end   avg_amount39_enquiryfee             
,case when max_amount39_enquiryfee             >50000.0  then 50000.0     else         max_amount39_enquiryfee                end   max_amount39_enquiryfee             
,case when sum_cnt39_enquiryfee                >26.0     then 26.0        else         sum_cnt39_enquiryfee                   end   sum_cnt39_enquiryfee                
,case when min_amount39_enquiryfee             >15000.0  then 15000.0     else         min_amount39_enquiryfee                end   min_amount39_enquiryfee             
,case when sum_amount23_enquiryfee             >343476.0 then 343476.0    else         sum_amount23_enquiryfee                end   sum_amount23_enquiryfee             
,case when avg_amount23_enquiryfee             >25124.0  then 25124.0     else         avg_amount23_enquiryfee                end   avg_amount23_enquiryfee             
,case when max_amount23_enquiryfee             >50000.0  then 50000.0     else         max_amount23_enquiryfee                end   max_amount23_enquiryfee             
,case when sum_cnt23_enquiryfee                >31.0     then 31.0        else         sum_cnt23_enquiryfee                   end   sum_cnt23_enquiryfee                
,case when min_amount23_enquiryfee             >17291.0  then 17291.0     else         min_amount23_enquiryfee                end   min_amount23_enquiryfee             
,case when sum_amount41_enquiryfee             >385952.0 then 385952.0    else         sum_amount41_enquiryfee                end   sum_amount41_enquiryfee             
,case when avg_amount41_enquiryfee             >27590.0  then 27590.0     else         avg_amount41_enquiryfee                end   avg_amount41_enquiryfee             
,case when max_amount41_enquiryfee             >53022.0  then 53022.0     else         max_amount41_enquiryfee                end   max_amount41_enquiryfee             
,case when sum_cnt41_enquiryfee                >30.0     then 30.0        else         sum_cnt41_enquiryfee                   end   sum_cnt41_enquiryfee                
,case when min_amount41_enquiryfee             >15976.0  then 15976.0     else         min_amount41_enquiryfee                end   min_amount41_enquiryfee             
,case when sum_amount22_enquiryfee             >341390.0 then 341390.0    else         sum_amount22_enquiryfee                end   sum_amount22_enquiryfee             
,case when avg_amount22_enquiryfee             >25560.0  then 25560.0     else         avg_amount22_enquiryfee                end   avg_amount22_enquiryfee             
,case when max_amount22_enquiryfee             >50000.0  then 50000.0     else         max_amount22_enquiryfee                end   max_amount22_enquiryfee             
,case when sum_cnt22_enquiryfee                >30.0     then 30.0        else         sum_cnt22_enquiryfee                   end   sum_cnt22_enquiryfee                
,case when min_amount22_enquiryfee             >16848.0  then 16848.0     else         min_amount22_enquiryfee                end   min_amount22_enquiryfee             
,case when sum_amount21_enquiryfee             >348895.0 then 348895.0    else         sum_amount21_enquiryfee                end   sum_amount21_enquiryfee             
,case when avg_amount21_enquiryfee             >25174.0  then 25174.0     else         avg_amount21_enquiryfee                end   avg_amount21_enquiryfee             
,case when max_amount21_enquiryfee             >50000.0  then 50000.0     else         max_amount21_enquiryfee                end   max_amount21_enquiryfee             
,case when sum_cnt21_enquiryfee                >31.0     then 31.0         else        sum_cnt21_enquiryfee                   end   sum_cnt21_enquiryfee                
,case when min_amount21_enquiryfee             >16994.0  then 16994.0      else        min_amount21_enquiryfee                end   min_amount21_enquiryfee             
,case when sum_amount42_enquiryfee             >409770.0 then 409770.0     else        sum_amount42_enquiryfee                end   sum_amount42_enquiryfee             
,case when avg_amount42_enquiryfee             >27111.0  then 27111.0      else        avg_amount42_enquiryfee                end   avg_amount42_enquiryfee             
,case when max_amount42_enquiryfee             >52995.0  then 52995.0      else        max_amount42_enquiryfee                end   max_amount42_enquiryfee             
,case when sum_cnt42_enquiryfee                >33.0     then 33.0         else        sum_cnt42_enquiryfee                   end   sum_cnt42_enquiryfee                
,case when min_amount42_enquiryfee             >14990.0  then 14990.0      else        min_amount42_enquiryfee                end   min_amount42_enquiryfee            
,case when sum_amount40_enquiryfee             >265580.0 then 265580.0    else         sum_amount40_enquiryfee                end   sum_amount40_enquiryfee             
,case when avg_amount40_enquiryfee             >25190.0  then 25190.0     else         avg_amount40_enquiryfee                end   avg_amount40_enquiryfee             
,case when max_amount40_enquiryfee             >50000.0  then 50000.0      else        max_amount40_enquiryfee                end   max_amount40_enquiryfee             
,case when sum_cnt40_enquiryfee                >21.0     then 21.0         else        sum_cnt40_enquiryfee                   end   sum_cnt40_enquiryfee                
,case when min_amount40_enquiryfee             >14998.0  then 14998.0      else        min_amount40_enquiryfee                end   min_amount40_enquiryfee             
,case when sum_amount20_enquiryfee             >314155.0 then 314155.0     else        sum_amount20_enquiryfee                end   sum_amount20_enquiryfee             
,case when avg_amount20_enquiryfee             >24629.0  then 24629.0      else        avg_amount20_enquiryfee                end   avg_amount20_enquiryfee             
,case when max_amount20_enquiryfee             >49999.0  then 49999.0      else        max_amount20_enquiryfee                end   max_amount20_enquiryfee             
,case when sum_cnt20_enquiryfee                >29.0     then 29.0         else        sum_cnt20_enquiryfee                   end   sum_cnt20_enquiryfee               
,case when min_amount20_enquiryfee             >16486.0  then 16486.0      else        min_amount20_enquiryfee                end   min_amount20_enquiryfee                
,case when sum_amount43_enquiryfee             >321204.0 then 321204.0   else          sum_amount43_enquiryfee                end   sum_amount43_enquiryfee            
,case when avg_amount43_enquiryfee             >27009.0  then 27009.0    else          avg_amount43_enquiryfee                end   avg_amount43_enquiryfee             
,case when max_amount43_enquiryfee             >50120.0  then 50120.0    else          max_amount43_enquiryfee                end   max_amount43_enquiryfee             
,case when sum_cnt43_enquiryfee                >29.0     then 29.0       else          sum_cnt43_enquiryfee                   end   sum_cnt43_enquiryfee                
,case when min_amount43_enquiryfee             >15011.0  then 15011.0    else          min_amount43_enquiryfee                end   min_amount43_enquiryfee             
,case when sum_amount44_enquiryfee             >307760.0 then 307760.0   else          sum_amount44_enquiryfee                end   sum_amount44_enquiryfee             
,case when avg_amount44_enquiryfee             >26073.0  then 26073.0    else          avg_amount44_enquiryfee                end   avg_amount44_enquiryfee             
,case when max_amount44_enquiryfee             >50513.0  then 50513.0    else          max_amount44_enquiryfee                end   max_amount44_enquiryfee             
,case when sum_cnt44_enquiryfee                >27.0     then 27.0        else         sum_cnt44_enquiryfee                   end   sum_cnt44_enquiryfee                
,case when min_amount44_enquiryfee             >14988.0  then 14988.0     else         min_amount44_enquiryfee                end   min_amount44_enquiryfee             
,case when sum_amount19_enquiryfee             >287762.0 then 287762.0    else         sum_amount19_enquiryfee                end   sum_amount19_enquiryfee             
,case when avg_amount19_enquiryfee             >24263.0  then 24263.0     else         avg_amount19_enquiryfee                end   avg_amount19_enquiryfee             
,case when max_amount19_enquiryfee             >50000.0  then 50000.0     else         max_amount19_enquiryfee                end   max_amount19_enquiryfee             
,case when sum_cnt19_enquiryfee                >26.0     then 26.0        else         sum_cnt19_enquiryfee                   end   sum_cnt19_enquiryfee                
,case when min_amount19_enquiryfee             >15996.0  then 15996.0     else         min_amount19_enquiryfee                end   min_amount19_enquiryfee             
,case when sum_amount45_enquiryfee             >297565.0 then 297565.0    else         sum_amount45_enquiryfee                end   sum_amount45_enquiryfee             
,case when avg_amount45_enquiryfee             >25902.0  then 25902.0     else         avg_amount45_enquiryfee                end   avg_amount45_enquiryfee             
,case when max_amount45_enquiryfee             >50782.0  then 50782.0     else         max_amount45_enquiryfee                end   max_amount45_enquiryfee             
,case when sum_cnt45_enquiryfee                >24.0     then 24.0        else         sum_cnt45_enquiryfee                   end   sum_cnt45_enquiryfee                
,case when min_amount45_enquiryfee             >13998.0  then 13998.0     else         min_amount45_enquiryfee                end   min_amount45_enquiryfee             
,case when sum_amount18_enquiryfee             >309773.0 then 309773.0    else         sum_amount18_enquiryfee                end   sum_amount18_enquiryfee             
,case when avg_amount18_enquiryfee             >24717.0  then 24717.0     else         avg_amount18_enquiryfee                end   avg_amount18_enquiryfee             
,case when max_amount18_enquiryfee             >49999.0  then 49999.0     else         max_amount18_enquiryfee                end   max_amount18_enquiryfee             
,case when min_amount18_enquiryfee             >16251.0  then 16251.0     else         min_amount18_enquiryfee                end   min_amount18_enquiryfee             
,case when sum_cnt18_enquiryfee                >27.0     then 27.0        else         sum_cnt18_enquiryfee                   end   sum_cnt18_enquiryfee                
,case when sum_amount46_enquiryfee             >234267.0 then 234267.0    else         sum_amount46_enquiryfee                end   sum_amount46_enquiryfee             
,case when avg_amount46_enquiryfee             >24995.0  then 24995.0     else         avg_amount46_enquiryfee                end   avg_amount46_enquiryfee             
,case when max_amount46_enquiryfee             >50000.0  then 50000.0     else         max_amount46_enquiryfee                end   max_amount46_enquiryfee             
,case when sum_cnt46_enquiryfee                >19.0     then 19.0        else         sum_cnt46_enquiryfee                   end   sum_cnt46_enquiryfee                
,case when min_amount46_enquiryfee             >13000.0  then 13000.0     else         min_amount46_enquiryfee                end   min_amount46_enquiryfee             
,case when sum_amount47_enquiryfee             >217278.0 then 217278.0    else         sum_amount47_enquiryfee                end   sum_amount47_enquiryfee             
,case when avg_amount47_enquiryfee             >24979.0  then 24979.0     else         avg_amount47_enquiryfee                end   avg_amount47_enquiryfee             
,case when max_amount47_enquiryfee             >50000.0  then 50000.0     else         max_amount47_enquiryfee                end   max_amount47_enquiryfee             
,case when sum_cnt47_enquiryfee                >18.0     then 18.0        else         sum_cnt47_enquiryfee                   end   sum_cnt47_enquiryfee                
,case when min_amount47_enquiryfee             >13836.0  then 13836.0     else         min_amount47_enquiryfee                end   min_amount47_enquiryfee             
,case when sum_amount17_enquiryfee             >277140.0 then 277140.0    else         sum_amount17_enquiryfee                end   sum_amount17_enquiryfee             
,case when avg_amount17_enquiryfee             >23249.0  then 23249.0     else         avg_amount17_enquiryfee                end   avg_amount17_enquiryfee             
,case when max_amount17_enquiryfee             >47952.0  then 47952.0     else         max_amount17_enquiryfee                end   max_amount17_enquiryfee             
,case when min_amount17_enquiryfee             >15080.0  then 15080.0     else         min_amount17_enquiryfee                end   min_amount17_enquiryfee             
,case when sum_cnt17_enquiryfee                >25.0     then 25.0        else         sum_cnt17_enquiryfee                   end   sum_cnt17_enquiryfee                
,case when sum_amount30_creditcardrepayments   >322043.0 then 322043.0    else         sum_amount30_creditcardrepayments      end   sum_amount30_creditcardrepayments   
,case when avg_amount30_creditcardrepayments   >27218.0  then 27218.0     else         avg_amount30_creditcardrepayments      end   avg_amount30_creditcardrepayments   
,case when max_amount30_creditcardrepayments   >50000.0  then 50000.0     else         max_amount30_creditcardrepayments      end   max_amount30_creditcardrepayments   
,case when min_amount30_creditcardrepayments   >19999.0  then 19999.0     else         min_amount30_creditcardrepayments      end   min_amount30_creditcardrepayments   
,case when sum_cnt30_creditcardrepayments      >27.0     then 27.0        else         sum_cnt30_creditcardrepayments         end   sum_cnt30_creditcardrepayments      
,case when sum_amount33_creditcardrepayments   >302843.0 then 302843.0    else         sum_amount33_creditcardrepayments      end   sum_amount33_creditcardrepayments   
,case when avg_amount33_creditcardrepayments   >26879.0  then 26879.0     else         avg_amount33_creditcardrepayments      end   avg_amount33_creditcardrepayments   
,case when max_amount33_creditcardrepayments   >50000.0  then 50000.0     else         max_amount33_creditcardrepayments      end   max_amount33_creditcardrepayments   
,case when min_amount33_creditcardrepayments   >19999.0  then 19999.0     else         min_amount33_creditcardrepayments      end   min_amount33_creditcardrepayments   
,case when sum_cnt33_creditcardrepayments      >27.0     then 27.0        else         sum_cnt33_creditcardrepayments         end   sum_cnt33_creditcardrepayments      
,case when sum_amount32_creditcardrepayments   >285119.0 then 285119.0    else         sum_amount32_creditcardrepayments      end   sum_amount32_creditcardrepayments   
,case when avg_amount32_creditcardrepayments   >27057.0  then 27057.0     else         avg_amount32_creditcardrepayments      end   avg_amount32_creditcardrepayments   
,case when max_amount32_creditcardrepayments   >50000.0  then 50000.0     else         max_amount32_creditcardrepayments      end   max_amount32_creditcardrepayments   
,case when min_amount32_creditcardrepayments   >19976.0  then 19976.0     else         min_amount32_creditcardrepayments      end   min_amount32_creditcardrepayments   
,case when sum_cnt32_creditcardrepayments      >25.0     then 25.0        else         sum_cnt32_creditcardrepayments         end   sum_cnt32_creditcardrepayments      
,case when sum_amount31_creditcardrepayments   >293118.0 then 293118.0    else         sum_amount31_creditcardrepayments      end   sum_amount31_creditcardrepayments   
,case when avg_amount31_creditcardrepayments   >27670.0  then 27670.0     else         avg_amount31_creditcardrepayments      end   avg_amount31_creditcardrepayments   
,case when max_amount31_creditcardrepayments   >50000.0  then 50000.0     else         max_amount31_creditcardrepayments      end   max_amount31_creditcardrepayments   
,case when min_amount31_creditcardrepayments   >19999.0  then 19999.0     else         min_amount31_creditcardrepayments      end   min_amount31_creditcardrepayments   
,case when sum_cnt31_creditcardrepayments      >25.0     then 25.0        else         sum_cnt31_creditcardrepayments         end   sum_cnt31_creditcardrepayments      
,case when sum_amount34_creditcardrepayments   >276367.0 then 276367.0    else         sum_amount34_creditcardrepayments      end   sum_amount34_creditcardrepayments   
,case when avg_amount34_creditcardrepayments   >26085.0  then 26085.0     else         avg_amount34_creditcardrepayments      end   avg_amount34_creditcardrepayments   
,case when max_amount34_creditcardrepayments   >50000.0  then 50000.0     else         max_amount34_creditcardrepayments      end   max_amount34_creditcardrepayments   
,case when min_amount34_creditcardrepayments   >18539.0  then 18539.0     else         min_amount34_creditcardrepayments      end   min_amount34_creditcardrepayments   
,case when sum_cnt34_creditcardrepayments      >24.0     then 24.0        else         sum_cnt34_creditcardrepayments         end   sum_cnt34_creditcardrepayments      
,case when sum_amount29_creditcardrepayments   >298529.0 then 298529.0    else         sum_amount29_creditcardrepayments      end   sum_amount29_creditcardrepayments   
,case when avg_amount29_creditcardrepayments   >26887.0  then 26887.0     else         avg_amount29_creditcardrepayments      end   avg_amount29_creditcardrepayments   
,case when max_amount29_creditcardrepayments   >50000.0  then 50000.0     else         max_amount29_creditcardrepayments      end   max_amount29_creditcardrepayments   
,case when min_amount29_creditcardrepayments   >19904.0  then 19904.0     else         min_amount29_creditcardrepayments      end   min_amount29_creditcardrepayments   
,case when sum_cnt29_creditcardrepayments      >26.0     then 26.0        else         sum_cnt29_creditcardrepayments         end   sum_cnt29_creditcardrepayments      
,case when sum_amount35_creditcardrepayments   >278960.0 then 278960.0    else         sum_amount35_creditcardrepayments      end   sum_amount35_creditcardrepayments   
,case when avg_amount35_creditcardrepayments   >26533.0  then 26533.0     else         avg_amount35_creditcardrepayments      end   avg_amount35_creditcardrepayments   
,case when max_amount35_creditcardrepayments   >50000.0  then 50000.0     else         max_amount35_creditcardrepayments      end   max_amount35_creditcardrepayments   
,case when min_amount35_creditcardrepayments   >19086.0  then 19086.0     else         min_amount35_creditcardrepayments      end   min_amount35_creditcardrepayments   
,case when sum_cnt35_creditcardrepayments      >24.0     then 24.0        else         sum_cnt35_creditcardrepayments         end   sum_cnt35_creditcardrepayments      
,case when sum_amount37_creditcardrepayments   >265421.0 then 265421.0    else         sum_amount37_creditcardrepayments      end   sum_amount37_creditcardrepayments   
,case when avg_amount37_creditcardrepayments   >25795.0  then 25795.0     else         avg_amount37_creditcardrepayments      end   avg_amount37_creditcardrepayments   
,case when max_amount37_creditcardrepayments   >49999.0  then 49999.0     else         max_amount37_creditcardrepayments      end   max_amount37_creditcardrepayments   
,case when min_amount37_creditcardrepayments   >17988.0  then 17988.0     else         min_amount37_creditcardrepayments      end   min_amount37_creditcardrepayments   
,case when sum_cnt37_creditcardrepayments      >23.0     then 23.0        else         sum_cnt37_creditcardrepayments         end   sum_cnt37_creditcardrepayments      
,case when sum_amount36_creditcardrepayments   >260304.0 then 260304.0    else         sum_amount36_creditcardrepayments      end   sum_amount36_creditcardrepayments   
,case when avg_amount36_creditcardrepayments   >26084.0  then 26084.0     else         avg_amount36_creditcardrepayments      end   avg_amount36_creditcardrepayments   
,case when max_amount36_creditcardrepayments   >50000.0  then 50000.0     else         max_amount36_creditcardrepayments      end   max_amount36_creditcardrepayments   
,case when min_amount36_creditcardrepayments   >18474.0  then 18474.0     else         min_amount36_creditcardrepayments      end   min_amount36_creditcardrepayments   
,case when sum_cnt36_creditcardrepayments      >23.0     then 23.0        else         sum_cnt36_creditcardrepayments         end   sum_cnt36_creditcardrepayments      
,case when sum_amount39_creditcardrepayments   >267121.0 then 267121.0    else         sum_amount39_creditcardrepayments      end   sum_amount39_creditcardrepayments   
,case when avg_amount39_creditcardrepayments   >25161.0  then 25161.0     else         avg_amount39_creditcardrepayments      end   avg_amount39_creditcardrepayments   
,case when max_amount39_creditcardrepayments   >50000.0  then 50000.0     else         max_amount39_creditcardrepayments      end   max_amount39_creditcardrepayments   
,case when min_amount39_creditcardrepayments   >17171.0  then 17171.0     else         min_amount39_creditcardrepayments      end   min_amount39_creditcardrepayments   
,case when sum_cnt39_creditcardrepayments      >24.0     then 24.0        else         sum_cnt39_creditcardrepayments         end   sum_cnt39_creditcardrepayments      
,case when sum_amount38_creditcardrepayments   >258667.0 then 258667.0    else         sum_amount38_creditcardrepayments      end   sum_amount38_creditcardrepayments   
,case when avg_amount38_creditcardrepayments   >25985.0  then 25985.0     else         avg_amount38_creditcardrepayments      end   avg_amount38_creditcardrepayments   
,case when max_amount38_creditcardrepayments   >50000.0  then 50000.0     else         max_amount38_creditcardrepayments      end   max_amount38_creditcardrepayments   
,case when min_amount38_creditcardrepayments   >18509.0  then 18509.0     else         min_amount38_creditcardrepayments      end   min_amount38_creditcardrepayments   
,case when sum_cnt38_creditcardrepayments      >23.0     then 23.0        else         sum_cnt38_creditcardrepayments         end   sum_cnt38_creditcardrepayments      
,case when sum_amount27_creditcardrepayments   >329898.0 then 329898.0    else         sum_amount27_creditcardrepayments      end   sum_amount27_creditcardrepayments   
,case when avg_amount27_creditcardrepayments   >27140.0  then 27140.0     else         avg_amount27_creditcardrepayments      end   avg_amount27_creditcardrepayments   
,case when max_amount27_creditcardrepayments   >50000.0  then 50000.0     else         max_amount27_creditcardrepayments      end   max_amount27_creditcardrepayments   
,case when min_amount27_creditcardrepayments   >19999.0  then 19999.0      else        min_amount27_creditcardrepayments      end   min_amount27_creditcardrepayments   
,case when sum_cnt27_creditcardrepayments      >28.0     then 28.0         else        sum_cnt27_creditcardrepayments         end   sum_cnt27_creditcardrepayments      
,case when sum_amount41_creditcardrepayments   >284930.0 then 284930.0     else        sum_amount41_creditcardrepayments      end   sum_amount41_creditcardrepayments   
,case when avg_amount41_creditcardrepayments   >26386.0  then 26386.0      else        avg_amount41_creditcardrepayments      end   avg_amount41_creditcardrepayments   
,case when max_amount41_creditcardrepayments   >50000.0  then 50000.0      else        max_amount41_creditcardrepayments      end   max_amount41_creditcardrepayments   
,case when min_amount41_creditcardrepayments   >17989.0  then 17989.0      else        min_amount41_creditcardrepayments      end   min_amount41_creditcardrepayments   
,case when sum_cnt41_creditcardrepayments      >24.0     then 24.0         else        sum_cnt41_creditcardrepayments         end   sum_cnt41_creditcardrepayments     
,case when sum_amount42_creditcardrepayments   >273405.0 then 273405.0    else         sum_amount42_creditcardrepayments      end   sum_amount42_creditcardrepayments   
,case when avg_amount42_creditcardrepayments   >26235.0  then 26235.0     else         avg_amount42_creditcardrepayments      end   avg_amount42_creditcardrepayments   
,case when max_amount42_creditcardrepayments   >50000.0  then 50000.0      else        max_amount42_creditcardrepayments      end   max_amount42_creditcardrepayments   
,case when min_amount42_creditcardrepayments   >17964.0  then 17964.0      else        min_amount42_creditcardrepayments      end   min_amount42_creditcardrepayments   
,case when sum_cnt42_creditcardrepayments      >23.0     then 23.0         else        sum_cnt42_creditcardrepayments         end   sum_cnt42_creditcardrepayments      
,case when sum_amount28_creditcardrepayments   >261963.0 then 261963.0     else        sum_amount28_creditcardrepayments      end   sum_amount28_creditcardrepayments   
,case when avg_amount28_creditcardrepayments   >26665.0  then 26665.0      else        avg_amount28_creditcardrepayments      end   avg_amount28_creditcardrepayments   
,case when max_amount28_creditcardrepayments   >50000.0  then 50000.0      else        max_amount28_creditcardrepayments      end   max_amount28_creditcardrepayments   
,case when min_amount28_creditcardrepayments   >19896.0  then 19896.0      else        min_amount28_creditcardrepayments      end   min_amount28_creditcardrepayments  
,case when sum_cnt28_creditcardrepayments      >22.0     then 22.0         else        sum_cnt28_creditcardrepayments         end   sum_cnt28_creditcardrepayments         
,case when sum_amount26_creditcardrepayments   >294313.0 then 294313.0   else          sum_amount26_creditcardrepayments      end   sum_amount26_creditcardrepayments  
,case when avg_amount26_creditcardrepayments   >26984.0  then 26984.0    else          avg_amount26_creditcardrepayments      end   avg_amount26_creditcardrepayments   
,case when max_amount26_creditcardrepayments   >50000.0  then 50000.0    else          max_amount26_creditcardrepayments      end   max_amount26_creditcardrepayments   
,case when min_amount26_creditcardrepayments   >19995.0  then 19995.0    else          min_amount26_creditcardrepayments      end   min_amount26_creditcardrepayments   
,case when sum_cnt26_creditcardrepayments      >25.0     then 25.0       else          sum_cnt26_creditcardrepayments         end   sum_cnt26_creditcardrepayments      
,case when sum_amount25_creditcardrepayments   >287866.0 then 287866.0   else          sum_amount25_creditcardrepayments      end   sum_amount25_creditcardrepayments   
,case when avg_amount25_creditcardrepayments   >26498.0  then 26498.0    else          avg_amount25_creditcardrepayments      end   avg_amount25_creditcardrepayments   
,case when max_amount25_creditcardrepayments   >50000.0  then 50000.0    else          max_amount25_creditcardrepayments      end   max_amount25_creditcardrepayments   
,case when min_amount25_creditcardrepayments   >19804.0  then 19804.0     else         min_amount25_creditcardrepayments      end   min_amount25_creditcardrepayments   
,case when sum_cnt25_creditcardrepayments      >24.0     then 24.0        else         sum_cnt25_creditcardrepayments         end   sum_cnt25_creditcardrepayments      
,case when sum_amount43_creditcardrepayments   >265410.0 then 265410.0    else         sum_amount43_creditcardrepayments      end   sum_amount43_creditcardrepayments   
,case when avg_amount43_creditcardrepayments   >28259.0  then 28259.0     else         avg_amount43_creditcardrepayments      end   avg_amount43_creditcardrepayments   
,case when max_amount43_creditcardrepayments   >50000.0  then 50000.0     else         max_amount43_creditcardrepayments      end   max_amount43_creditcardrepayments   
,case when min_amount43_creditcardrepayments   >19109.0  then 19109.0     else         min_amount43_creditcardrepayments      end   min_amount43_creditcardrepayments   
,case when sum_cnt43_creditcardrepayments      >21.0     then 21.0        else         sum_cnt43_creditcardrepayments         end   sum_cnt43_creditcardrepayments      
,case when sum_amount24_creditcardrepayments   >273453.0 then 273453.0    else         sum_amount24_creditcardrepayments      end   sum_amount24_creditcardrepayments   
,case when avg_amount24_creditcardrepayments   >26498.0  then 26498.0     else         avg_amount24_creditcardrepayments      end   avg_amount24_creditcardrepayments   
,case when max_amount24_creditcardrepayments   >50000.0  then 50000.0     else         max_amount24_creditcardrepayments      end   max_amount24_creditcardrepayments   
,case when min_amount24_creditcardrepayments   >19877.0  then 19877.0     else         min_amount24_creditcardrepayments      end   min_amount24_creditcardrepayments   
,case when sum_cnt24_creditcardrepayments      >23.0     then 23.0        else         sum_cnt24_creditcardrepayments         end   sum_cnt24_creditcardrepayments      
,case when sum_amount40_creditcardrepayments   >221210.0 then 221210.0    else         sum_amount40_creditcardrepayments      end   sum_amount40_creditcardrepayments   
,case when avg_amount40_creditcardrepayments   >25063.0  then 25063.0     else         avg_amount40_creditcardrepayments      end   avg_amount40_creditcardrepayments   
,case when max_amount40_creditcardrepayments   >50000.0  then 50000.0     else         max_amount40_creditcardrepayments      end   max_amount40_creditcardrepayments   
,case when min_amount40_creditcardrepayments   >17316.0  then 17316.0     else         min_amount40_creditcardrepayments      end   min_amount40_creditcardrepayments   
,case when sum_cnt40_creditcardrepayments      >19.0     then 19.0        else         sum_cnt40_creditcardrepayments         end   sum_cnt40_creditcardrepayments      
,case when sum_amount44_creditcardrepayments   >251037.0 then 251037.0    else         sum_amount44_creditcardrepayments      end   sum_amount44_creditcardrepayments   
,case when avg_amount44_creditcardrepayments   >26983.0  then 26983.0     else         avg_amount44_creditcardrepayments      end   avg_amount44_creditcardrepayments   
,case when max_amount44_creditcardrepayments   >50000.0  then 50000.0     else         max_amount44_creditcardrepayments      end   max_amount44_creditcardrepayments   
,case when min_amount44_creditcardrepayments   >18025.0  then 18025.0     else         min_amount44_creditcardrepayments      end   min_amount44_creditcardrepayments   
,case when sum_cnt44_creditcardrepayments      >20.0     then 20.0        else         sum_cnt44_creditcardrepayments         end   sum_cnt44_creditcardrepayments      
,case when sum_amount23_creditcardrepayments   >280500.0 then 280500.0    else         sum_amount23_creditcardrepayments      end   sum_amount23_creditcardrepayments   
,case when avg_amount23_creditcardrepayments   >24985.0  then 24985.0     else         avg_amount23_creditcardrepayments      end   avg_amount23_creditcardrepayments   
,case when max_amount23_creditcardrepayments   >49999.0  then 49999.0     else         max_amount23_creditcardrepayments      end   max_amount23_creditcardrepayments   
,case when min_amount23_creditcardrepayments   >18738.0  then 18738.0     else         min_amount23_creditcardrepayments      end   min_amount23_creditcardrepayments   
,case when sum_cnt23_creditcardrepayments      >25.0     then 25.0        else         sum_cnt23_creditcardrepayments         end   sum_cnt23_creditcardrepayments      
,case when sum_amount45_creditcardrepayments   >260063.0 then 260063.0    else         sum_amount45_creditcardrepayments      end   sum_amount45_creditcardrepayments   
,case when avg_amount45_creditcardrepayments   >26296.0  then 26296.0     else         avg_amount45_creditcardrepayments      end   avg_amount45_creditcardrepayments   
,case when max_amount45_creditcardrepayments   >50000.0  then 50000.0     else         max_amount45_creditcardrepayments      end   max_amount45_creditcardrepayments   
,case when min_amount45_creditcardrepayments   >16990.0  then 16990.0     else         min_amount45_creditcardrepayments      end   min_amount45_creditcardrepayments   
,case when sum_cnt45_creditcardrepayments      >21.0     then 21.0        else         sum_cnt45_creditcardrepayments         end   sum_cnt45_creditcardrepayments      
,case when sum_amount22_creditcardrepayments   >257995.0 then 257995.0    else         sum_amount22_creditcardrepayments      end   sum_amount22_creditcardrepayments   
,case when avg_amount22_creditcardrepayments   >24999.0  then 24999.0     else         avg_amount22_creditcardrepayments      end   avg_amount22_creditcardrepayments   
,case when max_amount22_creditcardrepayments   >49999.0  then 49999.0     else         max_amount22_creditcardrepayments      end   max_amount22_creditcardrepayments   
,case when min_amount22_creditcardrepayments   >18515.0  then 18515.0     else         min_amount22_creditcardrepayments      end   min_amount22_creditcardrepayments   
,case when sum_cnt22_creditcardrepayments      >23.0     then 23.0        else         sum_cnt22_creditcardrepayments         end   sum_cnt22_creditcardrepayments      
,case when sum_amount46_creditcardrepayments   >237074.0 then 237074.0    else         sum_amount46_creditcardrepayments      end   sum_amount46_creditcardrepayments   
,case when avg_amount46_creditcardrepayments   >26146.0  then 26146.0     else         avg_amount46_creditcardrepayments      end   avg_amount46_creditcardrepayments   
,case when max_amount46_creditcardrepayments   >50000.0  then 50000.0     else         max_amount46_creditcardrepayments      end   max_amount46_creditcardrepayments   
,case when min_amount46_creditcardrepayments   >16498.0  then 16498.0     else         min_amount46_creditcardrepayments      end   min_amount46_creditcardrepayments   
,case when sum_cnt46_creditcardrepayments      >19.0     then 19.0        else         sum_cnt46_creditcardrepayments         end   sum_cnt46_creditcardrepayments      
,case when sum_amount21_creditcardrepayments   >257188.0 then 257188.0    else         sum_amount21_creditcardrepayments      end   sum_amount21_creditcardrepayments   
,case when avg_amount21_creditcardrepayments   >24748.0  then 24748.0     else         avg_amount21_creditcardrepayments      end   avg_amount21_creditcardrepayments   
,case when max_amount21_creditcardrepayments   >50000.0  then 50000.0     else         max_amount21_creditcardrepayments      end   max_amount21_creditcardrepayments   
,case when min_amount21_creditcardrepayments   >18000.0  then 18000.0     else         min_amount21_creditcardrepayments      end   min_amount21_creditcardrepayments   
,case when sum_cnt21_creditcardrepayments      >23.0     then 23.0        else         sum_cnt21_creditcardrepayments         end   sum_cnt21_creditcardrepayments      
,case when sum_amount47_creditcardrepayments   >232579.0 then 232579.0    else         sum_amount47_creditcardrepayments      end   sum_amount47_creditcardrepayments   
,case when avg_amount47_creditcardrepayments   >25624.0  then 25624.0     else         avg_amount47_creditcardrepayments      end   avg_amount47_creditcardrepayments   
,case when max_amount47_creditcardrepayments   >50000.0  then 50000.0     else         max_amount47_creditcardrepayments      end   max_amount47_creditcardrepayments   
,case when min_amount47_creditcardrepayments   >16016.0  then 16016.0     else         min_amount47_creditcardrepayments      end   min_amount47_creditcardrepayments   
,case when sum_cnt47_creditcardrepayments      >19.0     then 19.0        else         sum_cnt47_creditcardrepayments         end   sum_cnt47_creditcardrepayments      
,case when sum_amount20_creditcardrepayments   >237129.0 then 237129.0    else         sum_amount20_creditcardrepayments      end   sum_amount20_creditcardrepayments   
,case when avg_amount20_creditcardrepayments   >24393.0  then 24393.0     else         avg_amount20_creditcardrepayments      end   avg_amount20_creditcardrepayments   
,case when max_amount20_creditcardrepayments   >50000.0  then 50000.0     else         max_amount20_creditcardrepayments      end   max_amount20_creditcardrepayments   
,case when min_amount20_creditcardrepayments   >17374.0  then 17374.0     else         min_amount20_creditcardrepayments      end   min_amount20_creditcardrepayments   
,case when sum_cnt20_creditcardrepayments      >21.0     then 21.0        else         sum_cnt20_creditcardrepayments         end   sum_cnt20_creditcardrepayments      
,case when sum_amount48_creditcardrepayments   >198270.0 then 198270.0    else         sum_amount48_creditcardrepayments      end   sum_amount48_creditcardrepayments   
,case when avg_amount48_creditcardrepayments   >24998.0  then 24998.0     else         avg_amount48_creditcardrepayments      end   avg_amount48_creditcardrepayments   
,case when max_amount48_creditcardrepayments   >50000.0  then 50000.0     else         max_amount48_creditcardrepayments      end   max_amount48_creditcardrepayments   
,case when min_amount48_creditcardrepayments   >14999.0  then 14999.0     else         min_amount48_creditcardrepayments      end   min_amount48_creditcardrepayments   
,case when sum_cnt48_creditcardrepayments      >17.0     then 17.0        else         sum_cnt48_creditcardrepayments         end   sum_cnt48_creditcardrepayments      
,case when sum_amount49_creditcardrepayments   >201192.0 then 201192.0    else         sum_amount49_creditcardrepayments      end   sum_amount49_creditcardrepayments   
,case when avg_amount49_creditcardrepayments   >24218.0  then 24218.0     else         avg_amount49_creditcardrepayments      end   avg_amount49_creditcardrepayments   
,case when max_amount49_creditcardrepayments   >50000.0  then 50000.0     else         max_amount49_creditcardrepayments      end   max_amount49_creditcardrepayments   
,case when min_amount49_creditcardrepayments   >14963.0  then 14963.0     else         min_amount49_creditcardrepayments      end   min_amount49_creditcardrepayments   
,case when sum_cnt49_creditcardrepayments      >17.0     then 17.0        else         sum_cnt49_creditcardrepayments         end   sum_cnt49_creditcardrepayments      
,case when sum_amount19_creditcardrepayments   >218671.0 then 218671.0    else         sum_amount19_creditcardrepayments      end   sum_amount19_creditcardrepayments   
,case when avg_amount19_creditcardrepayments   >24411.0  then 24411.0     else         avg_amount19_creditcardrepayments      end   avg_amount19_creditcardrepayments   
,case when max_amount19_creditcardrepayments   >49998.0  then 49998.0     else         max_amount19_creditcardrepayments      end   max_amount19_creditcardrepayments   
,case when min_amount19_creditcardrepayments   >17376.0  then 17376.0     else         min_amount19_creditcardrepayments      end   min_amount19_creditcardrepayments   
,case when sum_cnt19_creditcardrepayments      >20.0     then 20.0        else         sum_cnt19_creditcardrepayments         end   sum_cnt19_creditcardrepayments      
,case when sum_amount50_creditcardrepayments   >182216.0 then 182216.0    else         sum_amount50_creditcardrepayments      end   sum_amount50_creditcardrepayments   
,case when avg_amount50_creditcardrepayments   >23651.0  then 23651.0     else         avg_amount50_creditcardrepayments      end   avg_amount50_creditcardrepayments   
,case when max_amount50_creditcardrepayments   >49999.0  then 49999.0     else         max_amount50_creditcardrepayments      end   max_amount50_creditcardrepayments   
,case when min_amount50_creditcardrepayments   >14422.0  then 14422.0     else         min_amount50_creditcardrepayments      end   min_amount50_creditcardrepayments   
,case when sum_cnt50_creditcardrepayments      >16.0     then 16.0        else         sum_cnt50_creditcardrepayments         end   sum_cnt50_creditcardrepayments      
,case when sum_amount51_creditcardrepayments   >183484.0 then 183484.0    else         sum_amount51_creditcardrepayments      end   sum_amount51_creditcardrepayments   
,case when avg_amount51_creditcardrepayments   >22999.0  then 22999.0     else         avg_amount51_creditcardrepayments      end   avg_amount51_creditcardrepayments   
,case when max_amount51_creditcardrepayments   >49709.0  then 49709.0     else         max_amount51_creditcardrepayments      end   max_amount51_creditcardrepayments   
,case when min_amount51_creditcardrepayments   >13674.0  then 13674.0     else         min_amount51_creditcardrepayments      end   min_amount51_creditcardrepayments   
,case when sum_cnt51_creditcardrepayments      >16.0     then 16.0        else         sum_cnt51_creditcardrepayments         end   sum_cnt51_creditcardrepayments      
,case when sum_amount18_creditcardrepayments   >212084.0 then 212084.0    else         sum_amount18_creditcardrepayments      end   sum_amount18_creditcardrepayments   
,case when avg_amount18_creditcardrepayments   >23906.0  then 23906.0     else         avg_amount18_creditcardrepayments      end   avg_amount18_creditcardrepayments   
,case when max_amount18_creditcardrepayments   >48994.0  then 48994.0     else         max_amount18_creditcardrepayments      end   max_amount18_creditcardrepayments   
,case when min_amount18_creditcardrepayments   >17996.0  then 17996.0     else         min_amount18_creditcardrepayments      end   min_amount18_creditcardrepayments   
,case when sum_cnt18_creditcardrepayments      >20.0     then 20.0        else         sum_cnt18_creditcardrepayments         end   sum_cnt18_creditcardrepayments      
,case when sum_amount53_creditcardrepayments   >157783.0 then 157783.0    else         sum_amount53_creditcardrepayments      end   sum_amount53_creditcardrepayments   
,case when avg_amount53_creditcardrepayments   >21673.0  then 21673.0     else         avg_amount53_creditcardrepayments      end   avg_amount53_creditcardrepayments   
,case when max_amount53_creditcardrepayments   >47956.0  then 47956.0     else         max_amount53_creditcardrepayments      end   max_amount53_creditcardrepayments   
,case when min_amount53_creditcardrepayments   >12810.0  then 12810.0      else        min_amount53_creditcardrepayments      end   min_amount53_creditcardrepayments   
,case when sum_cnt53_creditcardrepayments      >15.0     then 15.0         else        sum_cnt53_creditcardrepayments         end   sum_cnt53_creditcardrepayments      
,case when sum_amount52_creditcardrepayments   >137234.0 then 137234.0     else        sum_amount52_creditcardrepayments      end   sum_amount52_creditcardrepayments   
,case when avg_amount52_creditcardrepayments   >21986.0  then 21986.0      else        avg_amount52_creditcardrepayments      end   avg_amount52_creditcardrepayments   
,case when max_amount52_creditcardrepayments   >46865.0  then 46865.0      else        max_amount52_creditcardrepayments      end   max_amount52_creditcardrepayments   
,case when min_amount52_creditcardrepayments   >13711.0  then 13711.0      else        min_amount52_creditcardrepayments      end   min_amount52_creditcardrepayments   
,case when sum_cnt52_creditcardrepayments      >13.0     then 13.0         else        sum_cnt52_creditcardrepayments         end   sum_cnt52_creditcardrepayments     
,case when sum_amount17_creditcardrepayments   >189407.0 then 189407.0    else         sum_amount17_creditcardrepayments      end   sum_amount17_creditcardrepayments   
,case when avg_amount17_creditcardrepayments   >22738.0  then 22738.0     else         avg_amount17_creditcardrepayments      end   avg_amount17_creditcardrepayments   
,case when max_amount17_creditcardrepayments   >45002.0  then 45002.0      else        max_amount17_creditcardrepayments      end   max_amount17_creditcardrepayments   
,case when min_amount17_creditcardrepayments   >16905.0  then 16905.0      else        min_amount17_creditcardrepayments      end   min_amount17_creditcardrepayments   
,case when sum_cnt17_creditcardrepayments      >17.0     then 17.0         else        sum_cnt17_creditcardrepayments         end   sum_cnt17_creditcardrepayments      
,case when sum_amount54_creditcardrepayments   >145986.0 then 145986.0     else        sum_amount54_creditcardrepayments      end   sum_amount54_creditcardrepayments   
,case when avg_amount54_creditcardrepayments   >20691.0  then 20691.0      else        avg_amount54_creditcardrepayments      end   avg_amount54_creditcardrepayments   
,case when max_amount54_creditcardrepayments   >46280.0  then 46280.0      else        max_amount54_creditcardrepayments      end   max_amount54_creditcardrepayments   
,case when min_amount54_creditcardrepayments   >11648.0  then 11648.0      else        min_amount54_creditcardrepayments      end   min_amount54_creditcardrepayments  
,case when sum_cnt54_creditcardrepayments      >14.0     then 14.0         else        sum_cnt54_creditcardrepayments         end   sum_cnt54_creditcardrepayments         
,case when sum_amount55_creditcardrepayments   >128171.0 then 128171.0   else          sum_amount55_creditcardrepayments      end   sum_amount55_creditcardrepayments  
,case when avg_amount55_creditcardrepayments   >19784.0  then 19784.0    else          avg_amount55_creditcardrepayments      end   avg_amount55_creditcardrepayments   
,case when max_amount55_creditcardrepayments   >41923.0  then 41923.0    else          max_amount55_creditcardrepayments      end   max_amount55_creditcardrepayments   
,case when min_amount55_creditcardrepayments   >10976.0  then 10976.0    else          min_amount55_creditcardrepayments      end   min_amount55_creditcardrepayments   
,case when sum_cnt55_creditcardrepayments      >13.0     then 13.0       else          sum_cnt55_creditcardrepayments         end   sum_cnt55_creditcardrepayments      
,case when sum_amount56_creditcardrepayments   >115756.0 then 115756.0   else          sum_amount56_creditcardrepayments      end   sum_amount56_creditcardrepayments   
,case when avg_amount56_creditcardrepayments   >19904.0  then 19904.0    else          avg_amount56_creditcardrepayments      end   avg_amount56_creditcardrepayments   
,case when max_amount56_creditcardrepayments   >42772.0  then 42772.0    else          max_amount56_creditcardrepayments      end   max_amount56_creditcardrepayments   
,case when min_amount56_creditcardrepayments   >10000.0  then 10000.0     else         min_amount56_creditcardrepayments      end   min_amount56_creditcardrepayments   
,case when sum_cnt56_creditcardrepayments      >12.0     then 12.0        else         sum_cnt56_creditcardrepayments         end   sum_cnt56_creditcardrepayments      
,case when sum_amount57_creditcardrepayments   >111425.0 then 111425.0    else         sum_amount57_creditcardrepayments      end   sum_amount57_creditcardrepayments   
,case when avg_amount57_creditcardrepayments   >18979.0  then 18979.0     else         avg_amount57_creditcardrepayments      end   avg_amount57_creditcardrepayments   
,case when max_amount57_creditcardrepayments   >40113.0  then 40113.0     else         max_amount57_creditcardrepayments      end   max_amount57_creditcardrepayments   
,case when min_amount57_creditcardrepayments   >9999.0   then 9999.0      else         min_amount57_creditcardrepayments      end   min_amount57_creditcardrepayments   
,case when sum_cnt57_creditcardrepayments      >12.0     then 12.0        else         sum_cnt57_creditcardrepayments         end   sum_cnt57_creditcardrepayments      
,case when sum_amount16_creditcardrepayments   >141517.0 then 141517.0    else         sum_amount16_creditcardrepayments      end   sum_amount16_creditcardrepayments   
,case when avg_amount16_creditcardrepayments   >20865.0  then 20865.0     else         avg_amount16_creditcardrepayments      end   avg_amount16_creditcardrepayments   
,case when max_amount16_creditcardrepayments   >40000.0  then 40000.0     else         max_amount16_creditcardrepayments      end   max_amount16_creditcardrepayments   
,case when min_amount16_creditcardrepayments   >14998.0  then 14998.0     else         min_amount16_creditcardrepayments      end   min_amount16_creditcardrepayments   
,case when sum_cnt16_creditcardrepayments      >14.0     then 14.0        else         sum_cnt16_creditcardrepayments         end   sum_cnt16_creditcardrepayments      
,case when sum_amount15_creditcardrepayments   >149206.0 then 149206.0    else         sum_amount15_creditcardrepayments      end   sum_amount15_creditcardrepayments   
,case when avg_amount15_creditcardrepayments   >20094.0  then 20094.0     else         avg_amount15_creditcardrepayments      end   avg_amount15_creditcardrepayments   
,case when max_amount15_creditcardrepayments   >39999.0  then 39999.0     else         max_amount15_creditcardrepayments      end   max_amount15_creditcardrepayments   
,case when min_amount15_creditcardrepayments   >13586.0  then 13586.0     else         min_amount15_creditcardrepayments      end   min_amount15_creditcardrepayments   
,case when sum_cnt15_creditcardrepayments      >14.0     then 14.0        else         sum_cnt15_creditcardrepayments         end   sum_cnt15_creditcardrepayments      
,case when sum_amount58_creditcardrepayments   >92670.0  then 92670.0     else         sum_amount58_creditcardrepayments      end   sum_amount58_creditcardrepayments   
,case when avg_amount58_creditcardrepayments   >17702.0  then 17702.0     else         avg_amount58_creditcardrepayments      end   avg_amount58_creditcardrepayments   
,case when max_amount58_creditcardrepayments   >36962.0  then 36962.0     else         max_amount58_creditcardrepayments      end   max_amount58_creditcardrepayments   
,case when min_amount58_creditcardrepayments   >9895.0   then 9895.0      else         min_amount58_creditcardrepayments      end   min_amount58_creditcardrepayments   
,case when sum_cnt58_creditcardrepayments      >11.0     then 11.0        else         sum_cnt58_creditcardrepayments         end   sum_cnt58_creditcardrepayments      
,case when sum_amount59_creditcardrepayments   >84419.0  then 84419.0     else         sum_amount59_creditcardrepayments      end   sum_amount59_creditcardrepayments   
,case when avg_amount59_creditcardrepayments   >16820.0  then 16820.0     else         avg_amount59_creditcardrepayments      end   avg_amount59_creditcardrepayments   
,case when max_amount59_creditcardrepayments   >34921.0  then 34921.0     else         max_amount59_creditcardrepayments      end   max_amount59_creditcardrepayments   
,case when min_amount59_creditcardrepayments   >9338.0   then 9338.0      else         min_amount59_creditcardrepayments      end   min_amount59_creditcardrepayments   
,case when sum_cnt59_creditcardrepayments      >10.0     then 10.0        else         sum_cnt59_creditcardrepayments         end   sum_cnt59_creditcardrepayments      
,case when sum_amount60_creditcardrepayments   >71569.0  then 71569.0     else         sum_amount60_creditcardrepayments      end   sum_amount60_creditcardrepayments   
,case when avg_amount60_creditcardrepayments   >15929.0  then 15929.0     else         avg_amount60_creditcardrepayments      end   avg_amount60_creditcardrepayments   
,case when max_amount60_creditcardrepayments   >31087.   then 31087.      else         max_amount60_creditcardrepayments      end   max_amount60_creditcardrepayments   
,case when min_amount60_creditcardrepayments   >9104.0   then 9104.0      else         min_amount60_creditcardrepayments      end   min_amount60_creditcardrepayments   
,case when sum_cnt60_creditcardrepayments      >9.0      then 9.0         else         sum_cnt60_creditcardrepayments         end   sum_cnt60_creditcardrepayments      
,case when sum_amount14_creditcardrepayments   >99253.0  then 99253.0     else         sum_amount14_creditcardrepayments      end   sum_amount14_creditcardrepayments   
,case when avg_amount14_creditcardrepayments   >17509.0  then 17509.0     else         avg_amount14_creditcardrepayments      end   avg_amount14_creditcardrepayments   
,case when max_amount14_creditcardrepayments   >30000.0  then 30000.0     else         max_amount14_creditcardrepayments      end   max_amount14_creditcardrepayments   
,case when min_amount14_creditcardrepayments   >11246.0  then 11246.0     else         min_amount14_creditcardrepayments      end   min_amount14_creditcardrepayments   
,case when sum_cnt14_creditcardrepayments      >10.0     then 10.0        else         sum_cnt14_creditcardrepayments         end   sum_cnt14_creditcardrepayments      
,case when sum_amount13_creditcardrepayments   >63018.0  then 63018.0     else         sum_amount13_creditcardrepayments      end   sum_amount13_creditcardrepayments   
,case when avg_amount13_creditcardrepayments   >13259.0  then 13259.0     else         avg_amount13_creditcardrepayments      end   avg_amount13_creditcardrepayments   
,case when max_amount13_creditcardrepayments   >22672.0  then 22672.0     else         max_amount13_creditcardrepayments      end   max_amount13_creditcardrepayments   
,case when min_amount13_creditcardrepayments   >7311.0   then 7311.0      else         min_amount13_creditcardrepayments      end   min_amount13_creditcardrepayments   
,case when sum_cnt13_creditcardrepayments      >8.0      then 8.0         else         sum_cnt13_creditcardrepayments         end   sum_cnt13_creditcardrepayments      
,case when sum_amount12_creditcardrepayments   >30634.0  then 30634.0     else         sum_amount12_creditcardrepayments      end   sum_amount12_creditcardrepayments   
,case when avg_amount12_creditcardrepayments   >8866.0   then 8866.0      else         avg_amount12_creditcardrepayments      end   avg_amount12_creditcardrepayments   
,case when max_amount12_creditcardrepayments   >14993.0  then 14993.0     else         max_amount12_creditcardrepayments      end   max_amount12_creditcardrepayments   
,case when min_amount12_creditcardrepayments   >3981.0   then 3981.0      else         min_amount12_creditcardrepayments      end   min_amount12_creditcardrepayments   
,case when sum_cnt12_creditcardrepayments      >4.0      then 4.0         else         sum_cnt12_creditcardrepayments         end   sum_cnt12_creditcardrepayments      

from phone_variable_yfq_creditcardrepayments_train_tc 
;             

create table  phone_variable_tnh_creditcardrepayments_train_tc_result_new as
select 
--label               
phone               
,  sum_amount1_creditcardrepayments 
,  avg_amount1_creditcardrepayments 
,  max_amount1_creditcardrepayments 
,  min_amount1_creditcardrepayments 
,  sum_cnt1_creditcardrepayments    
,  sum_amount5_creditcardrepayments 
,  avg_amount5_creditcardrepayments 
,  max_amount5_creditcardrepayments 
,  min_amount5_creditcardrepayments 
,  sum_cnt5_creditcardrepayments    
,  sum_amount2_creditcardrepayments 
,  avg_amount2_creditcardrepayments 
,  max_amount2_creditcardrepayments 
,  min_amount2_creditcardrepayments 
,  sum_cnt2_creditcardrepayments    
,  sum_amount4_creditcardrepayments 
,  avg_amount4_creditcardrepayments 
,  max_amount4_creditcardrepayments 
,  min_amount4_creditcardrepayments 
,  sum_cnt4_creditcardrepayments    
,  sum_amount6_creditcardrepayments 
,  avg_amount6_creditcardrepayments 
,  max_amount6_creditcardrepayments 
,  min_amount6_creditcardrepayments 
,  sum_cnt6_creditcardrepayments    
,  sum_amount7_creditcardrepayments 
,  avg_amount7_creditcardrepayments 
,  max_amount7_creditcardrepayments 
,  min_amount7_creditcardrepayments 
,  sum_cnt7_creditcardrepayments    
,  sum_amount9_creditcardrepayments 
,  avg_amount9_creditcardrepayments 
,  max_amount9_creditcardrepayments 
,  min_amount9_creditcardrepayments 
,  sum_cnt9_creditcardrepayments    
,  sum_amount3_creditcardrepayments 
,  avg_amount3_creditcardrepayments 
,  max_amount3_creditcardrepayments 
,  min_amount3_creditcardrepayments 
,  sum_cnt3_creditcardrepayments    
,  sum_amount8_creditcardrepayments 
,  avg_amount8_creditcardrepayments 
,  max_amount8_creditcardrepayments 
,  min_amount8_creditcardrepayments 
,  sum_cnt8_creditcardrepayments    
,  sum_amount10_creditcardrepayments
,  avg_amount10_creditcardrepayments
,  max_amount10_creditcardrepayments
,  min_amount10_creditcardrepayments
,  sum_cnt10_creditcardrepayments   
,  sum_amount11_creditcardrepayments
,  avg_amount11_creditcardrepayments
,  max_amount11_creditcardrepayments
,  min_amount11_creditcardrepayments
,  sum_cnt11_creditcardrepayments   
,  sum_amount12_creditcardrepayments
,  avg_amount12_creditcardrepayments
,  max_amount12_creditcardrepayments
,  min_amount12_creditcardrepayments
,  sum_cnt12_creditcardrepayments   
,  sum_amount14_creditcardrepayments
,  avg_amount14_creditcardrepayments
,  max_amount14_creditcardrepayments
,  min_amount14_creditcardrepayments
,  sum_cnt14_creditcardrepayments   
,  sum_amount13_creditcardrepayments
,  avg_amount13_creditcardrepayments
,  max_amount13_creditcardrepayments
,  min_amount13_creditcardrepayments
,  sum_cnt13_creditcardrepayments   
,  sum_amount15_creditcardrepayments
,  avg_amount15_creditcardrepayments
,  max_amount15_creditcardrepayments
,  min_amount15_creditcardrepayments
,  sum_cnt15_creditcardrepayments   
,  sum_amount16_creditcardrepayments
,  avg_amount16_creditcardrepayments
,  max_amount16_creditcardrepayments
,  min_amount16_creditcardrepayments
,  sum_cnt16_creditcardrepayments   
,  sum_amount17_creditcardrepayments
,  avg_amount17_creditcardrepayments
,  max_amount17_creditcardrepayments
,  min_amount17_creditcardrepayments
,  sum_cnt17_creditcardrepayments   
,  sum_amount18_creditcardrepayments
,  avg_amount18_creditcardrepayments
,  max_amount18_creditcardrepayments
,  min_amount18_creditcardrepayments
,  sum_cnt18_creditcardrepayments   
,  sum_amount19_creditcardrepayments
,  avg_amount19_creditcardrepayments
,  max_amount19_creditcardrepayments
,  min_amount19_creditcardrepayments
,  sum_cnt19_creditcardrepayments   
,  sum_amount20_creditcardrepayments
,  avg_amount20_creditcardrepayments
,  max_amount20_creditcardrepayments
,  min_amount20_creditcardrepayments
,  sum_cnt20_creditcardrepayments   
,  sum_amount21_creditcardrepayments
,  avg_amount21_creditcardrepayments
,  max_amount21_creditcardrepayments
,  min_amount21_creditcardrepayments
,  sum_cnt21_creditcardrepayments   
,  sum_amount22_creditcardrepayments
,  avg_amount22_creditcardrepayments
,  max_amount22_creditcardrepayments
,  min_amount22_creditcardrepayments
,  sum_cnt22_creditcardrepayments   
,  sum_amount23_creditcardrepayments
,  avg_amount23_creditcardrepayments
,  max_amount23_creditcardrepayments
,  min_amount23_creditcardrepayments
,  sum_cnt23_creditcardrepayments   
,  sum_amount24_creditcardrepayments
,  avg_amount24_creditcardrepayments
,  max_amount24_creditcardrepayments
,  min_amount24_creditcardrepayments
,  sum_cnt24_creditcardrepayments   
,  sum_amount25_creditcardrepayments
,  avg_amount25_creditcardrepayments
,  max_amount25_creditcardrepayments
,  min_amount25_creditcardrepayments
,  sum_cnt25_creditcardrepayments   
,  sum_amount26_creditcardrepayments
,  avg_amount26_creditcardrepayments
,  max_amount26_creditcardrepayments
,  min_amount26_creditcardrepayments
,  sum_cnt26_creditcardrepayments   
,  sum_amount27_creditcardrepayments
,  avg_amount27_creditcardrepayments
,  max_amount27_creditcardrepayments
,  min_amount27_creditcardrepayments
,  sum_cnt27_creditcardrepayments   
,  sum_amount28_creditcardrepayments
,  avg_amount28_creditcardrepayments
,  max_amount28_creditcardrepayments
,  min_amount28_creditcardrepayments
,  sum_cnt28_creditcardrepayments   
,  sum_amount29_creditcardrepayments
,  avg_amount29_creditcardrepayments
,  max_amount29_creditcardrepayments
,  min_amount29_creditcardrepayments
,  sum_cnt29_creditcardrepayments   
,  sum_amount30_creditcardrepayments
,  avg_amount30_creditcardrepayments
,  max_amount30_creditcardrepayments
,  min_amount30_creditcardrepayments
,  sum_cnt30_creditcardrepayments   
,  sum_amount31_creditcardrepayments
,  avg_amount31_creditcardrepayments
,  max_amount31_creditcardrepayments
,  min_amount31_creditcardrepayments
,  sum_cnt31_creditcardrepayments   
,  sum_amount32_creditcardrepayments
,  avg_amount32_creditcardrepayments
,  max_amount32_creditcardrepayments
,  min_amount32_creditcardrepayments
,  sum_cnt32_creditcardrepayments   
,  sum_amount33_creditcardrepayments
,  avg_amount33_creditcardrepayments
,  max_amount33_creditcardrepayments
,  min_amount33_creditcardrepayments
,  sum_cnt33_creditcardrepayments   
,  sum_amount34_creditcardrepayments
,  avg_amount34_creditcardrepayments
,  max_amount34_creditcardrepayments
,  min_amount34_creditcardrepayments
,  sum_cnt34_creditcardrepayments   
,  sum_amount35_creditcardrepayments
,  avg_amount35_creditcardrepayments
,  max_amount35_creditcardrepayments
,  min_amount35_creditcardrepayments
,  sum_cnt35_creditcardrepayments   
,  sum_amount36_creditcardrepayments
,  avg_amount36_creditcardrepayments
,  max_amount36_creditcardrepayments
,  min_amount36_creditcardrepayments
,  sum_cnt36_creditcardrepayments   
,  sum_amount37_creditcardrepayments
,  avg_amount37_creditcardrepayments
,  max_amount37_creditcardrepayments
,  min_amount37_creditcardrepayments
,  sum_cnt37_creditcardrepayments   
,  sum_amount38_creditcardrepayments
,  avg_amount38_creditcardrepayments
,  max_amount38_creditcardrepayments
,  min_amount38_creditcardrepayments
,  sum_cnt38_creditcardrepayments   
,  sum_amount39_creditcardrepayments
,  avg_amount39_creditcardrepayments
,  max_amount39_creditcardrepayments
,  min_amount39_creditcardrepayments
,  sum_cnt39_creditcardrepayments   
,  sum_amount40_creditcardrepayments
,  avg_amount40_creditcardrepayments
,  max_amount40_creditcardrepayments
,  min_amount40_creditcardrepayments
,  sum_cnt40_creditcardrepayments   
,  sum_amount41_creditcardrepayments
,  avg_amount41_creditcardrepayments
,  max_amount41_creditcardrepayments
,  min_amount41_creditcardrepayments
,  sum_cnt41_creditcardrepayments   
,  sum_amount42_creditcardrepayments
,  avg_amount42_creditcardrepayments
,  max_amount42_creditcardrepayments
,  min_amount42_creditcardrepayments
,  sum_cnt42_creditcardrepayments   
,  sum_amount43_creditcardrepayments
,  avg_amount43_creditcardrepayments
,  max_amount43_creditcardrepayments
,  min_amount43_creditcardrepayments
,  sum_cnt43_creditcardrepayments   
,  sum_amount44_creditcardrepayments
,  avg_amount44_creditcardrepayments
,  max_amount44_creditcardrepayments
,  min_amount44_creditcardrepayments
,  sum_cnt44_creditcardrepayments   
,  sum_amount1_enquiryfee           
,  avg_amount1_enquiryfee           
,  max_amount1_enquiryfee           
,  min_amount1_enquiryfee           
,  sum_cnt1_enquiryfee              
,  sum_amount2_enquiryfee           
,  avg_amount2_enquiryfee           
,  max_amount2_enquiryfee           
,  min_amount2_enquiryfee           
,  sum_cnt2_enquiryfee              
,  sum_amount3_enquiryfee           
,  avg_amount3_enquiryfee           
,  max_amount3_enquiryfee           
,  min_amount3_enquiryfee           
,  sum_cnt3_enquiryfee              
,  sum_amount4_enquiryfee           
,  avg_amount4_enquiryfee           
,  max_amount4_enquiryfee           
,  min_amount4_enquiryfee           
,  sum_cnt4_enquiryfee              
,  sum_amount5_enquiryfee           
,  avg_amount5_enquiryfee           
,  max_amount5_enquiryfee           
,  min_amount5_enquiryfee           
,  sum_cnt5_enquiryfee              
,  sum_amount6_enquiryfee           
,  avg_amount6_enquiryfee           
,  max_amount6_enquiryfee           
,  min_amount6_enquiryfee           
,  sum_cnt6_enquiryfee              
,  sum_amount7_enquiryfee           
,  avg_amount7_enquiryfee           
,  max_amount7_enquiryfee           
,  sum_cnt7_enquiryfee              
,  min_amount7_enquiryfee           
,  sum_amount8_enquiryfee           
,  avg_amount8_enquiryfee           
,  max_amount8_enquiryfee           
,  sum_cnt8_enquiryfee              
,  min_amount8_enquiryfee           
,  sum_amount9_enquiryfee           
,  avg_amount9_enquiryfee           
,  max_amount9_enquiryfee           
,  min_amount9_enquiryfee           
,  sum_cnt9_enquiryfee              
,  sum_amount10_enquiryfee          
,  avg_amount10_enquiryfee          
,  max_amount10_enquiryfee          
,  min_amount10_enquiryfee          
,  sum_cnt10_enquiryfee             
,  sum_amount11_enquiryfee          
,  avg_amount11_enquiryfee          
,  max_amount11_enquiryfee          
,  min_amount11_enquiryfee          
,  sum_cnt11_enquiryfee             
,  sum_amount12_enquiryfee          
,  avg_amount12_enquiryfee          
,  max_amount12_enquiryfee          
,  min_amount12_enquiryfee          
,  sum_cnt12_enquiryfee             
,  sum_amount13_enquiryfee          
,  avg_amount13_enquiryfee          
,  max_amount13_enquiryfee          
,  min_amount13_enquiryfee          
,  sum_cnt13_enquiryfee             
,  sum_amount14_enquiryfee          
,  avg_amount14_enquiryfee          
,  max_amount14_enquiryfee          
,  min_amount14_enquiryfee          
,  sum_cnt14_enquiryfee             
,  sum_amount15_enquiryfee          
,  avg_amount15_enquiryfee          
,  max_amount15_enquiryfee          
,  sum_cnt15_enquiryfee             
,  min_amount15_enquiryfee          
,  sum_amount16_enquiryfee          
,  avg_amount16_enquiryfee          
,  max_amount16_enquiryfee          
,  min_amount16_enquiryfee          
,  sum_cnt16_enquiryfee             
,  sum_amount17_enquiryfee          
,  avg_amount17_enquiryfee          
,  max_amount17_enquiryfee          
,  min_amount17_enquiryfee          
,  sum_cnt17_enquiryfee             
,  sum_amount18_enquiryfee          
,  avg_amount18_enquiryfee          
,  max_amount18_enquiryfee          
,  sum_cnt18_enquiryfee             
,  min_amount18_enquiryfee          
,  sum_amount19_enquiryfee          
,  avg_amount19_enquiryfee          
,  max_amount19_enquiryfee          
,  min_amount19_enquiryfee          
,  sum_cnt19_enquiryfee             
 ,  sum_amount20_enquiryfee          
 ,  avg_amount20_enquiryfee          
 ,  max_amount20_enquiryfee          
 ,  min_amount20_enquiryfee          
 ,  sum_cnt20_enquiryfee             
 ,  sum_amount21_enquiryfee          
 ,  avg_amount21_enquiryfee          
 ,  max_amount21_enquiryfee          
 ,  sum_cnt21_enquiryfee             
 ,  min_amount21_enquiryfee          
 ,  sum_amount22_enquiryfee          
 ,  avg_amount22_enquiryfee          
 ,  max_amount22_enquiryfee          
 ,  min_amount22_enquiryfee          
 ,  sum_cnt22_enquiryfee             
 ,  sum_amount23_enquiryfee          
 ,  avg_amount23_enquiryfee          
 ,  max_amount23_enquiryfee          
 ,  min_amount23_enquiryfee          
 ,  sum_cnt23_enquiryfee             
 ,  sum_amount24_enquiryfee          
 ,  avg_amount24_enquiryfee          
 ,  max_amount24_enquiryfee          
 ,  min_amount24_enquiryfee          
 ,  sum_cnt24_enquiryfee             
 ,  sum_amount25_enquiryfee          
 ,  avg_amount25_enquiryfee          
 ,  max_amount25_enquiryfee          
 ,  sum_cnt25_enquiryfee             
 ,  min_amount25_enquiryfee          
 ,  sum_amount26_enquiryfee          
 ,  avg_amount26_enquiryfee          
 ,  max_amount26_enquiryfee          
 ,  min_amount26_enquiryfee          
 ,  sum_cnt26_enquiryfee             
 ,  sum_amount27_enquiryfee          
 ,  avg_amount27_enquiryfee          
 ,  max_amount27_enquiryfee          
 ,  min_amount27_enquiryfee          
 ,  sum_cnt27_enquiryfee             
 ,  sum_amount28_enquiryfee          
 ,  avg_amount28_enquiryfee          
 ,  max_amount28_enquiryfee          
 ,  min_amount28_enquiryfee          
 ,  sum_cnt28_enquiryfee             
 ,  sum_amount29_enquiryfee          
 ,  avg_amount29_enquiryfee          
 ,  max_amount29_enquiryfee          
 ,  min_amount29_enquiryfee          
 ,  sum_cnt29_enquiryfee             
 ,  sum_amount30_enquiryfee          
 ,  avg_amount30_enquiryfee          
 ,  max_amount30_enquiryfee          
 ,  min_amount30_enquiryfee          
 ,  sum_cnt30_enquiryfee             
 ,  sum_amount31_enquiryfee          
 ,  avg_amount31_enquiryfee          
 ,  max_amount31_enquiryfee          
 ,  min_amount31_enquiryfee          
 ,  sum_cnt31_enquiryfee             
 ,  sum_amount32_enquiryfee          
 ,  avg_amount32_enquiryfee          
 ,  max_amount32_enquiryfee          
 ,  sum_cnt32_enquiryfee             
 ,  min_amount32_enquiryfee          
 ,  sum_amount33_enquiryfee          
 ,  avg_amount33_enquiryfee          
 ,  max_amount33_enquiryfee          
 ,  sum_cnt33_enquiryfee             
 ,  min_amount33_enquiryfee          
 

  
 
from phone_variable_tnh_creditcardrepayments_train_tc_result
where 
  (  sum_amount1_creditcardrepayments    <>0 
or   avg_amount1_creditcardrepayments <>0 
or   max_amount1_creditcardrepayments <>0 
or   min_amount1_creditcardrepayments <>0 
or   sum_cnt1_creditcardrepayments    <>0 
or   sum_amount5_creditcardrepayments <>0 
or   avg_amount5_creditcardrepayments <>0 
or   max_amount5_creditcardrepayments <>0 
or   min_amount5_creditcardrepayments <>0 
or   sum_cnt5_creditcardrepayments    <>0 
or   sum_amount2_creditcardrepayments <>0 
or   avg_amount2_creditcardrepayments <>0 
or   max_amount2_creditcardrepayments <>0 
or   min_amount2_creditcardrepayments <>0 
or   sum_cnt2_creditcardrepayments    <>0 
or   sum_amount4_creditcardrepayments <>0 
or   avg_amount4_creditcardrepayments <>0 
or   max_amount4_creditcardrepayments <>0 
or   min_amount4_creditcardrepayments <>0 
or   sum_cnt4_creditcardrepayments    <>0 
or   sum_amount6_creditcardrepayments <>0 
or   avg_amount6_creditcardrepayments <>0 
or   max_amount6_creditcardrepayments <>0 
or   min_amount6_creditcardrepayments <>0 
or   sum_cnt6_creditcardrepayments    <>0 
or   sum_amount7_creditcardrepayments <>0 
or   avg_amount7_creditcardrepayments <>0 
or   max_amount7_creditcardrepayments <>0 
or   min_amount7_creditcardrepayments <>0 
or   sum_cnt7_creditcardrepayments    <>0 
or   sum_amount9_creditcardrepayments <>0 
or   avg_amount9_creditcardrepayments <>0 
or   max_amount9_creditcardrepayments <>0 
or   min_amount9_creditcardrepayments <>0 
or   sum_cnt9_creditcardrepayments    <>0 
or   sum_amount3_creditcardrepayments <>0 
or   avg_amount3_creditcardrepayments <>0 
or   max_amount3_creditcardrepayments <>0 
or   min_amount3_creditcardrepayments <>0 
or   sum_cnt3_creditcardrepayments    <>0 
or   sum_amount8_creditcardrepayments <>0 
or   avg_amount8_creditcardrepayments <>0 
or   max_amount8_creditcardrepayments <>0 
or   min_amount8_creditcardrepayments <>0 
or   sum_cnt8_creditcardrepayments    <>0 
or   sum_amount10_creditcardrepayments<>0 
or   avg_amount10_creditcardrepayments<>0 
or   max_amount10_creditcardrepayments<>0 
or   min_amount10_creditcardrepayments<>0 
or   sum_cnt10_creditcardrepayments   <>0 
or   sum_amount11_creditcardrepayments<>0 
or   avg_amount11_creditcardrepayments<>0 
or   max_amount11_creditcardrepayments<>0 
or   min_amount11_creditcardrepayments<>0 
or   sum_cnt11_creditcardrepayments   <>0 
or   sum_amount12_creditcardrepayments<>0 
or   avg_amount12_creditcardrepayments<>0 
or   max_amount12_creditcardrepayments<>0 
or   min_amount12_creditcardrepayments<>0 
or   sum_cnt12_creditcardrepayments   <>0 
or   sum_amount14_creditcardrepayments<>0 
or   avg_amount14_creditcardrepayments<>0 
or   max_amount14_creditcardrepayments<>0 
or   min_amount14_creditcardrepayments<>0 
or   sum_cnt14_creditcardrepayments   <>0 
or   sum_amount13_creditcardrepayments<>0 
or   avg_amount13_creditcardrepayments<>0 
or   max_amount13_creditcardrepayments<>0 
or   min_amount13_creditcardrepayments<>0 
or   sum_cnt13_creditcardrepayments   <>0 
or   sum_amount15_creditcardrepayments<>0 
or   avg_amount15_creditcardrepayments<>0 
or   max_amount15_creditcardrepayments<>0 
or   min_amount15_creditcardrepayments<>0 
or   sum_cnt15_creditcardrepayments   <>0 
or   sum_amount16_creditcardrepayments<>0 
or   avg_amount16_creditcardrepayments<>0 
or   max_amount16_creditcardrepayments<>0 
or   min_amount16_creditcardrepayments<>0 
or   sum_cnt16_creditcardrepayments   <>0 
or   sum_amount17_creditcardrepayments<>0 
or   avg_amount17_creditcardrepayments<>0 
or   max_amount17_creditcardrepayments<>0 
or   min_amount17_creditcardrepayments<>0 
or   sum_cnt17_creditcardrepayments   <>0 
or   sum_amount18_creditcardrepayments<>0 
or   avg_amount18_creditcardrepayments<>0 
or   max_amount18_creditcardrepayments<>0 
or   min_amount18_creditcardrepayments<>0 
or   sum_cnt18_creditcardrepayments   <>0 
or   sum_amount19_creditcardrepayments<>0 
or   avg_amount19_creditcardrepayments<>0 
or   max_amount19_creditcardrepayments<>0 
or   min_amount19_creditcardrepayments<>0 
or   sum_cnt19_creditcardrepayments   <>0 
or   sum_amount20_creditcardrepayments<>0 
or   avg_amount20_creditcardrepayments<>0 
or   max_amount20_creditcardrepayments<>0 
or   min_amount20_creditcardrepayments<>0 
or   sum_cnt20_creditcardrepayments   <>0 
or   sum_amount21_creditcardrepayments<>0 
or   avg_amount21_creditcardrepayments<>0 
or   max_amount21_creditcardrepayments<>0 
or   min_amount21_creditcardrepayments<>0 
or   sum_cnt21_creditcardrepayments   <>0 
or   sum_amount22_creditcardrepayments <>0 
or   avg_amount22_creditcardrepayments<>0 
or   max_amount22_creditcardrepayments<>0 
or   min_amount22_creditcardrepayments<>0 
or   sum_cnt22_creditcardrepayments   <>0 
or   sum_amount23_creditcardrepayments<>0 
or   avg_amount23_creditcardrepayments<>0 
or   max_amount23_creditcardrepayments<>0 
or   min_amount23_creditcardrepayments<>0 
or   sum_cnt23_creditcardrepayments   <>0 
or   sum_amount24_creditcardrepayments<>0 
or   avg_amount24_creditcardrepayments<>0 
or   max_amount24_creditcardrepayments<>0 
or   min_amount24_creditcardrepayments<>0 
or   sum_cnt24_creditcardrepayments   <>0 
or   sum_amount25_creditcardrepayments<>0 
or   avg_amount25_creditcardrepayments<>0 
or   max_amount25_creditcardrepayments<>0 
or   min_amount25_creditcardrepayments<>0 
or   sum_cnt25_creditcardrepayments   <>0 
or   sum_amount26_creditcardrepayments<>0 
or   avg_amount26_creditcardrepayments<>0 
or   max_amount26_creditcardrepayments<>0 
or   min_amount26_creditcardrepayments<>0 
or   sum_cnt26_creditcardrepayments   <>0 
or   sum_amount27_creditcardrepayments<>0 
or   avg_amount27_creditcardrepayments<>0 
or   max_amount27_creditcardrepayments<>0 
or   min_amount27_creditcardrepayments<>0 
or   sum_cnt27_creditcardrepayments   <>0 
or   sum_amount28_creditcardrepayments<>0 
or   avg_amount28_creditcardrepayments<>0 
or   max_amount28_creditcardrepayments<>0 
or   min_amount28_creditcardrepayments<>0 
or   sum_cnt28_creditcardrepayments   <>0 
or   sum_amount29_creditcardrepayments<>0 
or   avg_amount29_creditcardrepayments<>0 
or   max_amount29_creditcardrepayments<>0 
or   min_amount29_creditcardrepayments<>0 
or   sum_cnt29_creditcardrepayments   <>0 
or   sum_amount30_creditcardrepayments<>0 
or   avg_amount30_creditcardrepayments<>0 
or   max_amount30_creditcardrepayments<>0 
or   min_amount30_creditcardrepayments<>0 
or   sum_cnt30_creditcardrepayments   <>0 
or   sum_amount31_creditcardrepayments<>0 
or   avg_amount31_creditcardrepayments<>0 
or   max_amount31_creditcardrepayments<>0 
or   min_amount31_creditcardrepayments<>0 
or   sum_cnt31_creditcardrepayments   <>0 
or   sum_amount32_creditcardrepayments<>0 
or   avg_amount32_creditcardrepayments<>0 
or   max_amount32_creditcardrepayments<>0 
or   min_amount32_creditcardrepayments<>0 
or   sum_cnt32_creditcardrepayments   <>0 
or   sum_amount33_creditcardrepayments<>0 
or   avg_amount33_creditcardrepayments<>0 
or   max_amount33_creditcardrepayments<>0 
or   min_amount33_creditcardrepayments<>0 
or   sum_cnt33_creditcardrepayments   <>0 
or   sum_amount34_creditcardrepayments<>0 
or   avg_amount34_creditcardrepayments<>0 
or   max_amount34_creditcardrepayments<>0 
or   min_amount34_creditcardrepayments<>0 
or   sum_cnt34_creditcardrepayments   <>0 
or   sum_amount35_creditcardrepayments<>0 
or   avg_amount35_creditcardrepayments<>0 
or   max_amount35_creditcardrepayments<>0 
or   min_amount35_creditcardrepayments<>0 
or   sum_cnt35_creditcardrepayments   <>0 
or   sum_amount36_creditcardrepayments<>0 
or   avg_amount36_creditcardrepayments<>0 
or   max_amount36_creditcardrepayments<>0 
or   min_amount36_creditcardrepayments<>0 
or   sum_cnt36_creditcardrepayments   <>0 
or   sum_amount37_creditcardrepayments<>0 
or   avg_amount37_creditcardrepayments<>0 
or   max_amount37_creditcardrepayments<>0 
or   min_amount37_creditcardrepayments<>0 
or   sum_cnt37_creditcardrepayments   <>0 
or   sum_amount38_creditcardrepayments<>0 
or   avg_amount38_creditcardrepayments<>0 
or   max_amount38_creditcardrepayments<>0 
or   min_amount38_creditcardrepayments<>0 
or   sum_cnt38_creditcardrepayments   <>0 
or   sum_amount39_creditcardrepayments<>0 
or   avg_amount39_creditcardrepayments<>0 
or   max_amount39_creditcardrepayments<>0 
or   min_amount39_creditcardrepayments<>0 
or   sum_cnt39_creditcardrepayments   <>0 
or   sum_amount40_creditcardrepayments<>0 
or   avg_amount40_creditcardrepayments<>0 
or   max_amount40_creditcardrepayments<>0 
or   min_amount40_creditcardrepayments<>0 
or   sum_cnt40_creditcardrepayments   <>0 
or   sum_amount41_creditcardrepayments<>0 
or   avg_amount41_creditcardrepayments<>0 
or   max_amount41_creditcardrepayments<>0 
or   min_amount41_creditcardrepayments<>0 
or   sum_cnt41_creditcardrepayments   <>0 
or   sum_amount42_creditcardrepayments<>0 
or   avg_amount42_creditcardrepayments<>0 
or   max_amount42_creditcardrepayments<>0 
or   min_amount42_creditcardrepayments<>0 
or   sum_cnt42_creditcardrepayments   <>0 
or   sum_amount43_creditcardrepayments <>0 
or   avg_amount43_creditcardrepayments<>0 
or   max_amount43_creditcardrepayments<>0 
or   min_amount43_creditcardrepayments<>0 
or   sum_cnt43_creditcardrepayments   <>0 
or   sum_amount44_creditcardrepayments<>0 
or   avg_amount44_creditcardrepayments<>0 
or   max_amount44_creditcardrepayments<>0 
or   min_amount44_creditcardrepayments<>0 
or   sum_cnt44_creditcardrepayments   <>0 
or   sum_amount1_enquiryfee           <>0 
or   avg_amount1_enquiryfee           <>0 
or   max_amount1_enquiryfee           <>0 
or   min_amount1_enquiryfee           <>0 
or   sum_cnt1_enquiryfee              <>0 
or   sum_amount2_enquiryfee           <>0 
or   avg_amount2_enquiryfee           <>0 
or   max_amount2_enquiryfee           <>0 
or   min_amount2_enquiryfee           <>0 
or   sum_cnt2_enquiryfee              <>0 
or   sum_amount3_enquiryfee           <>0 
or   avg_amount3_enquiryfee           <>0 
or   max_amount3_enquiryfee           <>0 
or   min_amount3_enquiryfee           <>0 
or   sum_cnt3_enquiryfee              <>0 
or   sum_amount4_enquiryfee           <>0 
or   avg_amount4_enquiryfee           <>0 
or   max_amount4_enquiryfee           <>0 
or   min_amount4_enquiryfee           <>0 
or   sum_cnt4_enquiryfee              <>0 
or   sum_amount5_enquiryfee           <>0 
or   avg_amount5_enquiryfee           <>0 
or   max_amount5_enquiryfee           <>0 
or   min_amount5_enquiryfee           <>0 
or   sum_cnt5_enquiryfee              <>0 
or   sum_amount6_enquiryfee           <>0 
or   avg_amount6_enquiryfee           <>0 
or   max_amount6_enquiryfee           <>0 
or   min_amount6_enquiryfee           <>0 
or   sum_cnt6_enquiryfee              <>0 
or   sum_amount7_enquiryfee           <>0 
or   avg_amount7_enquiryfee           <>0 
or   max_amount7_enquiryfee           <>0 
or   sum_cnt7_enquiryfee              <>0 
or   min_amount7_enquiryfee           <>0 
or   sum_amount8_enquiryfee           <>0 
or   avg_amount8_enquiryfee           <>0 
or   max_amount8_enquiryfee           <>0 
or   sum_cnt8_enquiryfee              <>0 
or   min_amount8_enquiryfee           <>0 
or   sum_amount9_enquiryfee           <>0 
or   avg_amount9_enquiryfee           <>0 
or   max_amount9_enquiryfee           <>0 
or   min_amount9_enquiryfee           <>0 
or   sum_cnt9_enquiryfee              <>0 
or   sum_amount10_enquiryfee          <>0 
or   avg_amount10_enquiryfee          <>0 
or   max_amount10_enquiryfee          <>0 
or   min_amount10_enquiryfee          <>0 
or   sum_cnt10_enquiryfee             <>0 
or   sum_amount11_enquiryfee          <>0 
or   avg_amount11_enquiryfee          <>0 
or   max_amount11_enquiryfee          <>0 
or   min_amount11_enquiryfee          <>0 
or   sum_cnt11_enquiryfee             <>0 
or   sum_amount12_enquiryfee          <>0 
or   avg_amount12_enquiryfee          <>0 
or   max_amount12_enquiryfee          <>0 
or   min_amount12_enquiryfee          <>0 
or   sum_cnt12_enquiryfee             <>0 
or   sum_amount13_enquiryfee          <>0 
or   avg_amount13_enquiryfee          <>0 
or   max_amount13_enquiryfee          <>0 
or   min_amount13_enquiryfee          <>0 
or   sum_cnt13_enquiryfee             <>0 
or   sum_amount14_enquiryfee          <>0 
or   avg_amount14_enquiryfee          <>0 
or   max_amount14_enquiryfee          <>0 
or   min_amount14_enquiryfee          <>0 
or   sum_cnt14_enquiryfee             <>0 
or   sum_amount15_enquiryfee          <>0 
or   avg_amount15_enquiryfee          <>0 
or   max_amount15_enquiryfee          <>0 
or   sum_cnt15_enquiryfee             <>0 
or   min_amount15_enquiryfee          <>0 
or   sum_amount16_enquiryfee          <>0 
or   avg_amount16_enquiryfee          <>0 
or   max_amount16_enquiryfee          <>0 
or   min_amount16_enquiryfee          <>0 
or   sum_cnt16_enquiryfee             <>0 
or   sum_amount17_enquiryfee          <>0 
or   avg_amount17_enquiryfee          <>0 
or   max_amount17_enquiryfee          <>0 
or   min_amount17_enquiryfee          <>0 
or   sum_cnt17_enquiryfee             <>0 
or   sum_amount18_enquiryfee          <>0 
or   avg_amount18_enquiryfee          <>0 
or   max_amount18_enquiryfee          <>0 
or   sum_cnt18_enquiryfee             <>0 
or   min_amount18_enquiryfee          <>0 
or   sum_amount19_enquiryfee          <>0 
or   avg_amount19_enquiryfee          <>0 
or   max_amount19_enquiryfee          <>0 
or   min_amount19_enquiryfee          <>0 
or   sum_cnt19_enquiryfee             <>0
or     sum_amount20_enquiryfee        <> 0 
or     avg_amount20_enquiryfee        <> 0 
or     max_amount20_enquiryfee        <> 0 
or     min_amount20_enquiryfee        <> 0 
or     sum_cnt20_enquiryfee           <> 0 
or     sum_amount21_enquiryfee        <> 0 
or     avg_amount21_enquiryfee        <> 0 
or     max_amount21_enquiryfee        <> 0 
or     sum_cnt21_enquiryfee           <> 0 
or     min_amount21_enquiryfee        <> 0 
or     sum_amount22_enquiryfee        <> 0 
or     avg_amount22_enquiryfee        <> 0 
or     max_amount22_enquiryfee        <> 0 
or     min_amount22_enquiryfee        <> 0 
or     sum_cnt22_enquiryfee           <> 0 
or     sum_amount23_enquiryfee        <> 0 
or     avg_amount23_enquiryfee        <> 0 
or     max_amount23_enquiryfee        <> 0 
or     min_amount23_enquiryfee        <> 0 
or     sum_cnt23_enquiryfee           <> 0 
or     sum_amount24_enquiryfee        <> 0 
or     avg_amount24_enquiryfee        <> 0 
or     max_amount24_enquiryfee        <> 0 
or     min_amount24_enquiryfee        <> 0 
or     sum_cnt24_enquiryfee           <> 0 
or     sum_amount25_enquiryfee        <> 0 
or     avg_amount25_enquiryfee        <> 0 
or     max_amount25_enquiryfee        <> 0 
or     sum_cnt25_enquiryfee           <> 0 
or     min_amount25_enquiryfee        <> 0 
or     sum_amount26_enquiryfee        <> 0 
or     avg_amount26_enquiryfee        <> 0 
or     max_amount26_enquiryfee        <> 0 
or     min_amount26_enquiryfee        <> 0 
or     sum_cnt26_enquiryfee           <> 0 
or     sum_amount27_enquiryfee        <> 0 
or     avg_amount27_enquiryfee        <> 0 
or     max_amount27_enquiryfee        <> 0 
or     min_amount27_enquiryfee        <> 0 
or     sum_cnt27_enquiryfee           <> 0 
or     sum_amount28_enquiryfee        <> 0 
or     avg_amount28_enquiryfee        <> 0 
or     max_amount28_enquiryfee        <> 0 
or     min_amount28_enquiryfee        <> 0 
or     sum_cnt28_enquiryfee           <> 0 
or     sum_amount29_enquiryfee        <> 0 
or     avg_amount29_enquiryfee        <> 0 
or     max_amount29_enquiryfee        <> 0 
or     min_amount29_enquiryfee        <> 0 
or     sum_cnt29_enquiryfee           <> 0 
or     sum_amount30_enquiryfee        <> 0 
or     avg_amount30_enquiryfee        <> 0 
or     max_amount30_enquiryfee        <> 0 
or     min_amount30_enquiryfee        <> 0 
or     sum_cnt30_enquiryfee           <> 0 
or     sum_amount31_enquiryfee        <> 0 
or     avg_amount31_enquiryfee        <> 0 
or     max_amount31_enquiryfee        <> 0 
or     min_amount31_enquiryfee        <> 0 
or     sum_cnt31_enquiryfee           <> 0 
or     sum_amount32_enquiryfee        <> 0 
or     avg_amount32_enquiryfee        <> 0 
or     max_amount32_enquiryfee        <> 0 
or     sum_cnt32_enquiryfee           <> 0 
or     min_amount32_enquiryfee        <> 0 
or     sum_amount33_enquiryfee        <> 0 
or     avg_amount33_enquiryfee        <> 0 
or     max_amount33_enquiryfee        <> 0 
or     sum_cnt33_enquiryfee           <> 0 
or     min_amount33_enquiryfee        <> 0    )
;

   
create table  phone_variable_yfq_creditcardrepayments_train_tc_result_new as
select 
--label
phone
, sum_amount20_creditcardrepayments         
, avg_amount20_creditcardrepayments         
, max_amount20_creditcardrepayments         
, min_amount20_creditcardrepayments         
, sum_cnt20_creditcardrepayments            
, sum_amount19_creditcardrepayments         
, avg_amount19_creditcardrepayments         
, max_amount19_creditcardrepayments         
, min_amount19_creditcardrepayments         
, sum_cnt19_creditcardrepayments            
, sum_amount21_creditcardrepayments         
, avg_amount21_creditcardrepayments         
, max_amount21_creditcardrepayments         
, min_amount21_creditcardrepayments         
, sum_cnt21_creditcardrepayments            
, sum_amount18_creditcardrepayments         
, avg_amount18_creditcardrepayments         
, max_amount18_creditcardrepayments         
, min_amount18_creditcardrepayments         
, sum_cnt18_creditcardrepayments            
, sum_amount22_creditcardrepayments         
, avg_amount22_creditcardrepayments         
, max_amount22_creditcardrepayments         
, min_amount22_creditcardrepayments         
, sum_cnt22_creditcardrepayments            
, sum_amount17_creditcardrepayments         
, avg_amount17_creditcardrepayments         
, max_amount17_creditcardrepayments         
, min_amount17_creditcardrepayments         
, sum_cnt17_creditcardrepayments            
, sum_amount23_creditcardrepayments         
, avg_amount23_creditcardrepayments         
, max_amount23_creditcardrepayments         
, min_amount23_creditcardrepayments         
, sum_cnt23_creditcardrepayments            
, sum_amount16_creditcardrepayments         
, avg_amount16_creditcardrepayments         
, max_amount16_creditcardrepayments         
, min_amount16_creditcardrepayments         
, sum_cnt16_creditcardrepayments            
, sum_amount24_creditcardrepayments         
, avg_amount24_creditcardrepayments         
, max_amount24_creditcardrepayments         
, min_amount24_creditcardrepayments         
, sum_cnt24_creditcardrepayments            
, sum_amount15_creditcardrepayments         
, avg_amount15_creditcardrepayments         
, max_amount15_creditcardrepayments         
, min_amount15_creditcardrepayments         
, sum_cnt15_creditcardrepayments            
, sum_amount25_creditcardrepayments         
, avg_amount25_creditcardrepayments         
, max_amount25_creditcardrepayments         
, min_amount25_creditcardrepayments         
, sum_cnt25_creditcardrepayments            
, sum_amount14_creditcardrepayments         
, avg_amount14_creditcardrepayments         
, max_amount14_creditcardrepayments         
, min_amount14_creditcardrepayments         
, sum_cnt14_creditcardrepayments            
, sum_amount26_creditcardrepayments         
, avg_amount26_creditcardrepayments         
, max_amount26_creditcardrepayments         
, min_amount26_creditcardrepayments         
, sum_cnt26_creditcardrepayments            
, sum_amount13_creditcardrepayments         
, avg_amount13_creditcardrepayments         
, max_amount13_creditcardrepayments         
, min_amount13_creditcardrepayments         
, sum_cnt13_creditcardrepayments            
, sum_amount27_creditcardrepayments         
, avg_amount27_creditcardrepayments         
, max_amount27_creditcardrepayments         
, min_amount27_creditcardrepayments         
, sum_cnt27_creditcardrepayments            
, sum_amount12_creditcardrepayments         
, avg_amount12_creditcardrepayments         
, max_amount12_creditcardrepayments         
, min_amount12_creditcardrepayments         
, sum_cnt12_creditcardrepayments            
, sum_amount28_creditcardrepayments         
, avg_amount28_creditcardrepayments         
, max_amount28_creditcardrepayments         
, min_amount28_creditcardrepayments         
, sum_cnt28_creditcardrepayments            
, sum_amount11_creditcardrepayments         
, avg_amount11_creditcardrepayments         
, max_amount11_creditcardrepayments         
, min_amount11_creditcardrepayments         
, sum_cnt11_creditcardrepayments            
, sum_amount29_creditcardrepayments         
, avg_amount29_creditcardrepayments         
, max_amount29_creditcardrepayments         
, min_amount29_creditcardrepayments         
, sum_cnt29_creditcardrepayments            
, sum_amount10_creditcardrepayments         
, avg_amount10_creditcardrepayments         
, max_amount10_creditcardrepayments         
, min_amount10_creditcardrepayments         
, sum_cnt10_creditcardrepayments            
, sum_amount30_creditcardrepayments         
, avg_amount30_creditcardrepayments         
, max_amount30_creditcardrepayments         
, min_amount30_creditcardrepayments         
, sum_cnt30_creditcardrepayments            
, sum_amount9_creditcardrepayments          
, avg_amount9_creditcardrepayments          
, max_amount9_creditcardrepayments          
, min_amount9_creditcardrepayments          
, sum_cnt9_creditcardrepayments             
, sum_amount31_creditcardrepayments         
, avg_amount31_creditcardrepayments         
, max_amount31_creditcardrepayments         
, min_amount31_creditcardrepayments         
, sum_cnt31_creditcardrepayments            
, sum_amount8_creditcardrepayments          
, avg_amount8_creditcardrepayments          
, max_amount8_creditcardrepayments          
, min_amount8_creditcardrepayments          
, sum_cnt8_creditcardrepayments             
, sum_amount32_creditcardrepayments         
, avg_amount32_creditcardrepayments         
, max_amount32_creditcardrepayments         
, min_amount32_creditcardrepayments         
, sum_cnt32_creditcardrepayments            
, sum_amount7_creditcardrepayments          
, avg_amount7_creditcardrepayments          
, max_amount7_creditcardrepayments          
, min_amount7_creditcardrepayments          
, sum_cnt7_creditcardrepayments             
, sum_amount33_creditcardrepayments         
, avg_amount33_creditcardrepayments         
, max_amount33_creditcardrepayments         
, min_amount33_creditcardrepayments         
, sum_cnt33_creditcardrepayments            
, sum_amount6_creditcardrepayments          
, avg_amount6_creditcardrepayments          
, max_amount6_creditcardrepayments          
, min_amount6_creditcardrepayments          
, sum_cnt6_creditcardrepayments             
, sum_amount34_creditcardrepayments         
, avg_amount34_creditcardrepayments         
, max_amount34_creditcardrepayments         
, min_amount34_creditcardrepayments         
, sum_cnt34_creditcardrepayments            
, sum_amount35_creditcardrepayments         
, avg_amount35_creditcardrepayments         
, max_amount35_creditcardrepayments         
, min_amount35_creditcardrepayments         
, sum_cnt35_creditcardrepayments            
, sum_amount5_creditcardrepayments          
, avg_amount5_creditcardrepayments          
, max_amount5_creditcardrepayments          
, min_amount5_creditcardrepayments          
, sum_cnt5_creditcardrepayments             
, sum_amount36_creditcardrepayments         
, avg_amount36_creditcardrepayments         
, max_amount36_creditcardrepayments         
, min_amount36_creditcardrepayments         
, sum_cnt36_creditcardrepayments            
, sum_amount4_creditcardrepayments          
, avg_amount4_creditcardrepayments          
, max_amount4_creditcardrepayments          
, min_amount4_creditcardrepayments          
, sum_cnt4_creditcardrepayments             
, sum_amount37_creditcardrepayments         
, avg_amount37_creditcardrepayments         
, max_amount37_creditcardrepayments         
, min_amount37_creditcardrepayments         
, sum_cnt37_creditcardrepayments            
, sum_amount3_creditcardrepayments          
, avg_amount3_creditcardrepayments          
, max_amount3_creditcardrepayments          
, min_amount3_creditcardrepayments          
, sum_cnt3_creditcardrepayments             
, sum_amount38_creditcardrepayments         
, avg_amount38_creditcardrepayments         
, max_amount38_creditcardrepayments         
, min_amount38_creditcardrepayments         
, sum_cnt38_creditcardrepayments            
, sum_amount2_creditcardrepayments          
, avg_amount2_creditcardrepayments          
, max_amount2_creditcardrepayments          
, min_amount2_creditcardrepayments          
, sum_cnt2_creditcardrepayments             
, sum_amount1_creditcardrepayments          
, avg_amount1_creditcardrepayments          
, max_amount1_creditcardrepayments          
, min_amount1_creditcardrepayments          
, sum_cnt1_creditcardrepayments             
, sum_amount39_creditcardrepayments         
, avg_amount39_creditcardrepayments         
, max_amount39_creditcardrepayments         
, min_amount39_creditcardrepayments         
, sum_cnt39_creditcardrepayments            
, sum_amount40_creditcardrepayments         
, avg_amount40_creditcardrepayments         
, max_amount40_creditcardrepayments         
, min_amount40_creditcardrepayments         
, sum_cnt40_creditcardrepayments            
, sum_amount41_creditcardrepayments         
, avg_amount41_creditcardrepayments         
, max_amount41_creditcardrepayments         
, min_amount41_creditcardrepayments         
, sum_cnt41_creditcardrepayments            
, sum_amount42_creditcardrepayments         
, avg_amount42_creditcardrepayments         
, max_amount42_creditcardrepayments         
, min_amount42_creditcardrepayments         
, sum_cnt42_creditcardrepayments            
, sum_amount43_creditcardrepayments         
, avg_amount43_creditcardrepayments         
, max_amount43_creditcardrepayments         
, min_amount43_creditcardrepayments         
, sum_cnt43_creditcardrepayments            
, sum_amount44_creditcardrepayments         
, avg_amount44_creditcardrepayments         
, max_amount44_creditcardrepayments         
, min_amount44_creditcardrepayments         
, sum_cnt44_creditcardrepayments            
, sum_amount45_creditcardrepayments         
, avg_amount45_creditcardrepayments         
, max_amount45_creditcardrepayments         
, min_amount45_creditcardrepayments         
, sum_cnt45_creditcardrepayments            
, sum_amount46_creditcardrepayments         
, avg_amount46_creditcardrepayments         
, max_amount46_creditcardrepayments         
, min_amount46_creditcardrepayments         
, sum_cnt46_creditcardrepayments            
, sum_amount47_creditcardrepayments         
, avg_amount47_creditcardrepayments         
, max_amount47_creditcardrepayments         
, min_amount47_creditcardrepayments         
, sum_cnt47_creditcardrepayments            
, sum_amount48_creditcardrepayments         
, avg_amount48_creditcardrepayments         
, max_amount48_creditcardrepayments         
, min_amount48_creditcardrepayments         
, sum_cnt48_creditcardrepayments            
, sum_amount49_creditcardrepayments         
, avg_amount49_creditcardrepayments         
, max_amount49_creditcardrepayments         
, min_amount49_creditcardrepayments         
, sum_cnt49_creditcardrepayments            
, sum_amount50_creditcardrepayments         
, avg_amount50_creditcardrepayments         
, max_amount50_creditcardrepayments         
, min_amount50_creditcardrepayments         
, sum_cnt50_creditcardrepayments            
, sum_amount51_creditcardrepayments         
, avg_amount51_creditcardrepayments         
, max_amount51_creditcardrepayments         
, min_amount51_creditcardrepayments         
, sum_cnt51_creditcardrepayments            
, sum_amount52_creditcardrepayments         
, avg_amount52_creditcardrepayments         
, max_amount52_creditcardrepayments         
, min_amount52_creditcardrepayments         
, sum_cnt52_creditcardrepayments            
, sum_amount53_creditcardrepayments         
, avg_amount53_creditcardrepayments         
, max_amount53_creditcardrepayments         
, min_amount53_creditcardrepayments         
, sum_cnt53_creditcardrepayments            
, sum_amount54_creditcardrepayments         
, avg_amount54_creditcardrepayments         
, max_amount54_creditcardrepayments         
, min_amount54_creditcardrepayments         
, sum_cnt54_creditcardrepayments            
, sum_amount17_enquiryfee                   
, avg_amount17_enquiryfee                   
, max_amount17_enquiryfee                   
, sum_cnt17_enquiryfee                      
, min_amount17_enquiryfee                   
, sum_amount16_enquiryfee                   
, avg_amount16_enquiryfee                   
, max_amount16_enquiryfee                   
, sum_cnt16_enquiryfee                      
, min_amount16_enquiryfee                   
, sum_amount18_enquiryfee                   
, avg_amount18_enquiryfee                   
, max_amount18_enquiryfee                   
, sum_cnt18_enquiryfee                      
, min_amount18_enquiryfee                   
, sum_amount15_enquiryfee                   
, avg_amount15_enquiryfee                   
, max_amount15_enquiryfee                   
, sum_cnt15_enquiryfee                      
, min_amount15_enquiryfee                   
, sum_amount19_enquiryfee                   
, avg_amount19_enquiryfee                   
, max_amount19_enquiryfee                   
, sum_cnt19_enquiryfee                      
, min_amount19_enquiryfee                   
, sum_amount20_enquiryfee                   
, avg_amount20_enquiryfee                   
, max_amount20_enquiryfee                   
, sum_cnt20_enquiryfee                      
, min_amount20_enquiryfee                   
, sum_amount14_enquiryfee                   
, avg_amount14_enquiryfee                   
, max_amount14_enquiryfee                   
, sum_cnt14_enquiryfee                      
, min_amount14_enquiryfee                   
, sum_amount21_enquiryfee                   
, avg_amount21_enquiryfee                   
, max_amount21_enquiryfee                   
, sum_cnt21_enquiryfee                      
, min_amount21_enquiryfee                   
, sum_amount13_enquiryfee                   
, avg_amount13_enquiryfee                   
, max_amount13_enquiryfee                   
, sum_cnt13_enquiryfee                      
, min_amount13_enquiryfee                   
, sum_amount22_enquiryfee                   
, avg_amount22_enquiryfee                   
, max_amount22_enquiryfee                   
, sum_cnt22_enquiryfee                      
, min_amount22_enquiryfee                   
, sum_amount12_enquiryfee                   
, avg_amount12_enquiryfee                   
, max_amount12_enquiryfee                   
, min_amount12_enquiryfee                   
, sum_cnt12_enquiryfee                      
, sum_amount23_enquiryfee                   
, avg_amount23_enquiryfee                   
, max_amount23_enquiryfee                   
, sum_cnt23_enquiryfee                      
, min_amount23_enquiryfee                   
, sum_amount11_enquiryfee                   
, avg_amount11_enquiryfee                   
, max_amount11_enquiryfee                   
, min_amount11_enquiryfee                   
, sum_cnt11_enquiryfee                      
, sum_amount24_enquiryfee                   
, avg_amount24_enquiryfee                   
, max_amount24_enquiryfee                   
, sum_cnt24_enquiryfee                      
, min_amount24_enquiryfee                   
, sum_amount10_enquiryfee                   
, avg_amount10_enquiryfee                   
, max_amount10_enquiryfee                   
, sum_cnt10_enquiryfee                      
, min_amount10_enquiryfee                   
, sum_amount9_enquiryfee                    
, avg_amount9_enquiryfee                    
, max_amount9_enquiryfee                    
, sum_cnt9_enquiryfee                       
, min_amount9_enquiryfee                    
, sum_amount25_enquiryfee                   
, avg_amount25_enquiryfee                   
, max_amount25_enquiryfee                   
, sum_cnt25_enquiryfee                      
, min_amount25_enquiryfee                   
, sum_amount8_enquiryfee                    
, avg_amount8_enquiryfee                    
, max_amount8_enquiryfee                    
, sum_cnt8_enquiryfee                       
, min_amount8_enquiryfee                    
, sum_amount26_enquiryfee                   
, avg_amount26_enquiryfee                   
, max_amount26_enquiryfee                   
, sum_cnt26_enquiryfee                      
, min_amount26_enquiryfee                   
, sum_amount7_enquiryfee                    
, avg_amount7_enquiryfee                    
, max_amount7_enquiryfee                    
, sum_cnt7_enquiryfee                       
, min_amount7_enquiryfee                    
, sum_amount27_enquiryfee                   
, avg_amount27_enquiryfee                   
, max_amount27_enquiryfee                   
, sum_cnt27_enquiryfee                      
, min_amount27_enquiryfee                   
, sum_amount6_enquiryfee                    
, avg_amount6_enquiryfee                    
, max_amount6_enquiryfee                    
, sum_cnt6_enquiryfee                       
, min_amount6_enquiryfee                    
, sum_amount28_enquiryfee                   
, avg_amount28_enquiryfee                   
, max_amount28_enquiryfee                   
, sum_cnt28_enquiryfee                      
, min_amount28_enquiryfee                   
, sum_amount5_enquiryfee                    
, avg_amount5_enquiryfee                    
, max_amount5_enquiryfee                    
, sum_cnt5_enquiryfee                       
, min_amount5_enquiryfee                    
, sum_amount4_enquiryfee                    
, avg_amount4_enquiryfee                    
, max_amount4_enquiryfee                    
, sum_cnt4_enquiryfee                       
, min_amount4_enquiryfee                    
, sum_amount29_enquiryfee                   
, avg_amount29_enquiryfee                   
, max_amount29_enquiryfee                   
, sum_cnt29_enquiryfee                      
, min_amount29_enquiryfee                   
  sum_amount3_enquiryfee                    
  avg_amount3_enquiryfee                    
  max_amount3_enquiryfee                    
  min_amount3_enquiryfee                    
  sum_cnt3_enquiryfee                       
  sum_amount30_enquiryfee                   
  avg_amount30_enquiryfee                   
  max_amount30_enquiryfee                   
  sum_cnt30_enquiryfee                      
  min_amount30_enquiryfee                   
  sum_amount2_enquiryfee                    
  avg_amount2_enquiryfee                    
  max_amount2_enquiryfee                    
  sum_cnt2_enquiryfee                       
  min_amount2_enquiryfee                    
  sum_amount1_enquiryfee                    
  avg_amount1_enquiryfee                    
  max_amount1_enquiryfee                    
  sum_cnt1_enquiryfee                       
  min_amount1_enquiryfee                    
  sum_amount31_enquiryfee                   
  avg_amount31_enquiryfee                   
  max_amount31_enquiryfee                   
  sum_cnt31_enquiryfee                      
  min_amount31_enquiryfee                   
  sum_amount32_enquiryfee                   
  avg_amount32_enquiryfee                   
  max_amount32_enquiryfee                   
  sum_cnt32_enquiryfee                      
  min_amount32_enquiryfee                   
  sum_amount33_enquiryfee                   
  avg_amount33_enquiryfee                   
  max_amount33_enquiryfee                   
  sum_cnt33_enquiryfee                      
  min_amount33_enquiryfee                   
  sum_amount34_enquiryfee                   
  avg_amount34_enquiryfee                   
  max_amount34_enquiryfee                   
  sum_cnt34_enquiryfee                      
  min_amount34_enquiryfee                   
  sum_amount35_enquiryfee                   
  avg_amount35_enquiryfee                   
  max_amount35_enquiryfee                   
  sum_cnt35_enquiryfee                      
  min_amount35_enquiryfee                   
  sum_amount36_enquiryfee                   
  avg_amount36_enquiryfee                   
  max_amount36_enquiryfee                   
  sum_cnt36_enquiryfee                      
  min_amount36_enquiryfee                   
  sum_amount37_enquiryfee                   
  avg_amount37_enquiryfee                   
  max_amount37_enquiryfee                   
  sum_cnt37_enquiryfee                      
  min_amount37_enquiryfee                   
  sum_amount38_enquiryfee                   
  avg_amount38_enquiryfee                   
  max_amount38_enquiryfee                   
  sum_cnt38_enquiryfee                      
  min_amount38_enquiryfee                   
  sum_amount39_enquiryfee                   
  avg_amount39_enquiryfee                   
  max_amount39_enquiryfee                   
  sum_cnt39_enquiryfee                      
  min_amount39_enquiryfee                   
  sum_amount40_enquiryfee                   
  avg_amount40_enquiryfee                   
  max_amount40_enquiryfee                   
  min_amount40_enquiryfee                   
  sum_cnt40_enquiryfee                      
  sum_amount41_enquiryfee                   
  avg_amount41_enquiryfee                   
  max_amount41_enquiryfee                   
  sum_cnt41_enquiryfee                      
  min_amount41_enquiryfee                   
  sum_amount42_enquiryfee                   
  avg_amount42_enquiryfee                   
  max_amount42_enquiryfee                   
  sum_cnt42_enquiryfee                      
  min_amount42_enquiryfee                   
                                     
                                     
               
               
    
 
from phone_variable_yfq_creditcardrepayments_train_tc_result
where 
  ( sum_amount20_creditcardrepayments             <>0
or  avg_amount20_creditcardrepayments         <>0
or  max_amount20_creditcardrepayments         <>0
or  min_amount20_creditcardrepayments         <>0
or  sum_cnt20_creditcardrepayments            <>0
or  sum_amount19_creditcardrepayments         <>0
or  avg_amount19_creditcardrepayments         <>0
or  max_amount19_creditcardrepayments         <>0
or  min_amount19_creditcardrepayments         <>0
or  sum_cnt19_creditcardrepayments            <>0
or  sum_amount21_creditcardrepayments         <>0
or  avg_amount21_creditcardrepayments         <>0
or  max_amount21_creditcardrepayments         <>0
or  min_amount21_creditcardrepayments         <>0
or  sum_cnt21_creditcardrepayments            <>0
or  sum_amount18_creditcardrepayments         <>0
or  avg_amount18_creditcardrepayments         <>0
or  max_amount18_creditcardrepayments         <>0
or  min_amount18_creditcardrepayments         <>0
or  sum_cnt18_creditcardrepayments            <>0
or  sum_amount22_creditcardrepayments         <>0
or  avg_amount22_creditcardrepayments         <>0
or  max_amount22_creditcardrepayments         <>0
or  min_amount22_creditcardrepayments         <>0
or  sum_cnt22_creditcardrepayments            <>0
or  sum_amount17_creditcardrepayments         <>0
or  avg_amount17_creditcardrepayments         <>0
or  max_amount17_creditcardrepayments         <>0
or  min_amount17_creditcardrepayments         <>0
or  sum_cnt17_creditcardrepayments            <>0
or  sum_amount23_creditcardrepayments         <>0
or  avg_amount23_creditcardrepayments         <>0
or  max_amount23_creditcardrepayments         <>0
or  min_amount23_creditcardrepayments         <>0
or  sum_cnt23_creditcardrepayments            <>0
or  sum_amount16_creditcardrepayments         <>0
or  avg_amount16_creditcardrepayments         <>0
or  max_amount16_creditcardrepayments         <>0
or  min_amount16_creditcardrepayments         <>0
or  sum_cnt16_creditcardrepayments            <>0
or  sum_amount24_creditcardrepayments         <>0
or  avg_amount24_creditcardrepayments         <>0
or  max_amount24_creditcardrepayments         <>0
or  min_amount24_creditcardrepayments         <>0
or  sum_cnt24_creditcardrepayments            <>0
or  sum_amount15_creditcardrepayments         <>0
or  avg_amount15_creditcardrepayments         <>0
or  max_amount15_creditcardrepayments         <>0
or  min_amount15_creditcardrepayments         <>0
or  sum_cnt15_creditcardrepayments            <>0
or  sum_amount25_creditcardrepayments         <>0
or  avg_amount25_creditcardrepayments         <>0
or  max_amount25_creditcardrepayments         <>0
or  min_amount25_creditcardrepayments         <>0
or  sum_cnt25_creditcardrepayments            <>0
or  sum_amount14_creditcardrepayments         <>0
or  avg_amount14_creditcardrepayments         <>0
or  max_amount14_creditcardrepayments         <>0
or  min_amount14_creditcardrepayments         <>0
or  sum_cnt14_creditcardrepayments            <>0
or  sum_amount26_creditcardrepayments         <>0
or  avg_amount26_creditcardrepayments         <>0
or  max_amount26_creditcardrepayments         <>0
or  min_amount26_creditcardrepayments         <>0
or  sum_cnt26_creditcardrepayments            <>0
or  sum_amount13_creditcardrepayments         <>0
or  avg_amount13_creditcardrepayments         <>0
or  max_amount13_creditcardrepayments         <>0
or  min_amount13_creditcardrepayments         <>0
or  sum_cnt13_creditcardrepayments            <>0
or  sum_amount27_creditcardrepayments         <>0
or  avg_amount27_creditcardrepayments         <>0
or  max_amount27_creditcardrepayments         <>0
or  min_amount27_creditcardrepayments         <>0
or  sum_cnt27_creditcardrepayments            <>0
or  sum_amount12_creditcardrepayments         <>0
or  avg_amount12_creditcardrepayments         <>0
or  max_amount12_creditcardrepayments         <>0
or  min_amount12_creditcardrepayments         <>0
or  sum_cnt12_creditcardrepayments            <>0
or  sum_amount28_creditcardrepayments         <>0
or  avg_amount28_creditcardrepayments         <>0
or  max_amount28_creditcardrepayments         <>0
or  min_amount28_creditcardrepayments         <>0
or  sum_cnt28_creditcardrepayments            <>0
or  sum_amount11_creditcardrepayments         <>0
or  avg_amount11_creditcardrepayments         <>0
or  max_amount11_creditcardrepayments         <>0
or  min_amount11_creditcardrepayments         <>0
or  sum_cnt11_creditcardrepayments            <>0
or  sum_amount29_creditcardrepayments         <>0
or  avg_amount29_creditcardrepayments         <>0
or  max_amount29_creditcardrepayments         <>0
or  min_amount29_creditcardrepayments         <>0
or  sum_cnt29_creditcardrepayments            <>0
or  sum_amount10_creditcardrepayments         <>0
or  avg_amount10_creditcardrepayments         <>0
or  max_amount10_creditcardrepayments         <>0
or  min_amount10_creditcardrepayments         <>0
or  sum_cnt10_creditcardrepayments            <>0
or  sum_amount30_creditcardrepayments         <>0
or  avg_amount30_creditcardrepayments         <>0
or  max_amount30_creditcardrepayments         <>0
or  min_amount30_creditcardrepayments         <>0
or  sum_cnt30_creditcardrepayments            <>0
or  sum_amount9_creditcardrepayments          <>0
or  avg_amount9_creditcardrepayments          <>0
or  max_amount9_creditcardrepayments          <>0
or  min_amount9_creditcardrepayments          <>0
or  sum_cnt9_creditcardrepayments             <>0
or  sum_amount31_creditcardrepayments         <>0
or  avg_amount31_creditcardrepayments         <>0
or  max_amount31_creditcardrepayments         <>0
or  min_amount31_creditcardrepayments         <>0
or  sum_cnt31_creditcardrepayments            <>0
or  sum_amount8_creditcardrepayments          <>0
or  avg_amount8_creditcardrepayments          <>0
or  max_amount8_creditcardrepayments          <>0
or  min_amount8_creditcardrepayments          <>0
or  sum_cnt8_creditcardrepayments             <>0
or  sum_amount32_creditcardrepayments         <>0
or  avg_amount32_creditcardrepayments         <>0
or  max_amount32_creditcardrepayments         <>0
or  min_amount32_creditcardrepayments         <>0
or  sum_cnt32_creditcardrepayments            <>0
or  sum_amount7_creditcardrepayments          <>0
or  avg_amount7_creditcardrepayments          <>0
or  max_amount7_creditcardrepayments          <>0
or  min_amount7_creditcardrepayments          <>0
or  sum_cnt7_creditcardrepayments             <>0
or  sum_amount33_creditcardrepayments         <>0
or  avg_amount33_creditcardrepayments         <>0
or  max_amount33_creditcardrepayments         <>0
or  min_amount33_creditcardrepayments         <>0
or  sum_cnt33_creditcardrepayments            <>0
or  sum_amount6_creditcardrepayments          <>0
or  avg_amount6_creditcardrepayments          <>0
or  max_amount6_creditcardrepayments          <>0
or  min_amount6_creditcardrepayments          <>0
or  sum_cnt6_creditcardrepayments             <>0
or  sum_amount34_creditcardrepayments         <>0
or  avg_amount34_creditcardrepayments         <>0
or  max_amount34_creditcardrepayments         <>0
or  min_amount34_creditcardrepayments         <>0
or  sum_cnt34_creditcardrepayments            <>0
or  sum_amount35_creditcardrepayments         <>0
or  avg_amount35_creditcardrepayments         <>0
or  max_amount35_creditcardrepayments         <>0
or  min_amount35_creditcardrepayments         <>0
or  sum_cnt35_creditcardrepayments            <>0
or  sum_amount5_creditcardrepayments          <>0
or  avg_amount5_creditcardrepayments          <>0
or  max_amount5_creditcardrepayments          <>0
or  min_amount5_creditcardrepayments          <>0
or  sum_cnt5_creditcardrepayments             <>0
or  sum_amount36_creditcardrepayments         <>0
or  avg_amount36_creditcardrepayments         <>0
or  max_amount36_creditcardrepayments         <>0
or  min_amount36_creditcardrepayments         <>0
or  sum_cnt36_creditcardrepayments            <>0
or  sum_amount4_creditcardrepayments          <>0
or  avg_amount4_creditcardrepayments          <>0
or  max_amount4_creditcardrepayments          <>0
or  min_amount4_creditcardrepayments          <>0
or  sum_cnt4_creditcardrepayments             <>0
or  sum_amount37_creditcardrepayments         <>0
or  avg_amount37_creditcardrepayments         <>0
or  max_amount37_creditcardrepayments         <>0
or  min_amount37_creditcardrepayments         <>0
or  sum_cnt37_creditcardrepayments            <>0
or  sum_amount3_creditcardrepayments          <>0
or  avg_amount3_creditcardrepayments          <>0
or  max_amount3_creditcardrepayments          <>0
or  min_amount3_creditcardrepayments          <>0
or  sum_cnt3_creditcardrepayments             <>0
or  sum_amount38_creditcardrepayments         <>0
or  avg_amount38_creditcardrepayments         <>0
or  max_amount38_creditcardrepayments         <>0
or  min_amount38_creditcardrepayments         <>0
or  sum_cnt38_creditcardrepayments            <>0
or  sum_amount2_creditcardrepayments          <>0
or  avg_amount2_creditcardrepayments          <>0
or  max_amount2_creditcardrepayments          <>0
or  min_amount2_creditcardrepayments          <>0
or  sum_cnt2_creditcardrepayments             <>0
or  sum_amount1_creditcardrepayments          <>0
or  avg_amount1_creditcardrepayments          <>0
or  max_amount1_creditcardrepayments          <>0
or  min_amount1_creditcardrepayments          <>0
or  sum_cnt1_creditcardrepayments             <>0
or  sum_amount39_creditcardrepayments         <>0
or  avg_amount39_creditcardrepayments         <>0
or  max_amount39_creditcardrepayments         <>0
or  min_amount39_creditcardrepayments         <>0
or  sum_cnt39_creditcardrepayments            <>0
or  sum_amount40_creditcardrepayments         <>0
or  avg_amount40_creditcardrepayments         <>0
or  max_amount40_creditcardrepayments         <>0
or  min_amount40_creditcardrepayments         <>0
or  sum_cnt40_creditcardrepayments            <>0
or  sum_amount41_creditcardrepayments         <>0
or  avg_amount41_creditcardrepayments         <>0
or  max_amount41_creditcardrepayments         <>0
or  min_amount41_creditcardrepayments         <>0
or  sum_cnt41_creditcardrepayments            <>0
or  sum_amount42_creditcardrepayments         <>0
or  avg_amount42_creditcardrepayments         <>0
or  max_amount42_creditcardrepayments         <>0
or  min_amount42_creditcardrepayments         <>0
or  sum_cnt42_creditcardrepayments            <>0
or  sum_amount43_creditcardrepayments         <>0
or  avg_amount43_creditcardrepayments         <>0
or  max_amount43_creditcardrepayments         <>0
or  min_amount43_creditcardrepayments         <>0
or  sum_cnt43_creditcardrepayments            <>0
or  sum_amount44_creditcardrepayments         <>0
or  avg_amount44_creditcardrepayments         <>0
or  max_amount44_creditcardrepayments         <>0
or  min_amount44_creditcardrepayments         <>0
or  sum_cnt44_creditcardrepayments            <>0
or  sum_amount45_creditcardrepayments         <>0
or  avg_amount45_creditcardrepayments         <>0
or  max_amount45_creditcardrepayments         <>0
or  min_amount45_creditcardrepayments         <>0
or  sum_cnt45_creditcardrepayments            <>0
or  sum_amount46_creditcardrepayments         <>0
or  avg_amount46_creditcardrepayments         <>0
or  max_amount46_creditcardrepayments         <>0
or  min_amount46_creditcardrepayments         <>0
or  sum_cnt46_creditcardrepayments            <>0
or  sum_amount47_creditcardrepayments         <>0
or  avg_amount47_creditcardrepayments         <>0
or  max_amount47_creditcardrepayments         <>0
or  min_amount47_creditcardrepayments         <>0
or  sum_cnt47_creditcardrepayments            <>0
or  sum_amount48_creditcardrepayments         <>0
or  avg_amount48_creditcardrepayments         <>0
or  max_amount48_creditcardrepayments         <>0
or  min_amount48_creditcardrepayments         <>0
or  sum_cnt48_creditcardrepayments            <>0
or  sum_amount49_creditcardrepayments         <>0
or  avg_amount49_creditcardrepayments         <>0
or  max_amount49_creditcardrepayments         <>0
or  min_amount49_creditcardrepayments         <>0
or  sum_cnt49_creditcardrepayments            <>0
or  sum_amount50_creditcardrepayments         <>0
or  avg_amount50_creditcardrepayments         <>0
or  max_amount50_creditcardrepayments         <>0
or  min_amount50_creditcardrepayments         <>0
or  sum_cnt50_creditcardrepayments            <>0
or  sum_amount51_creditcardrepayments         <>0
or  avg_amount51_creditcardrepayments         <>0
or  max_amount51_creditcardrepayments         <>0
or  min_amount51_creditcardrepayments         <>0
or  sum_cnt51_creditcardrepayments            <>0
or  sum_amount52_creditcardrepayments         <>0
or  avg_amount52_creditcardrepayments         <>0
or  max_amount52_creditcardrepayments         <>0
or  min_amount52_creditcardrepayments         <>0
or  sum_cnt52_creditcardrepayments            <>0
or  sum_amount53_creditcardrepayments         <>0
or  avg_amount53_creditcardrepayments         <>0
or  max_amount53_creditcardrepayments         <>0
or  min_amount53_creditcardrepayments         <>0
or  sum_cnt53_creditcardrepayments            <>0
or  sum_amount54_creditcardrepayments         <>0
or  avg_amount54_creditcardrepayments         <>0
or  max_amount54_creditcardrepayments         <>0
or  min_amount54_creditcardrepayments         <>0
or  sum_cnt54_creditcardrepayments            <>0
or  sum_amount17_enquiryfee                   <>0
or  avg_amount17_enquiryfee                   <>0
or  max_amount17_enquiryfee                   <>0
or  sum_cnt17_enquiryfee                      <>0
or  min_amount17_enquiryfee                   <>0
or  sum_amount16_enquiryfee                   <>0
or  avg_amount16_enquiryfee                   <>0
or  max_amount16_enquiryfee                   <>0
or  sum_cnt16_enquiryfee                      <>0
or  min_amount16_enquiryfee                   <>0
or  sum_amount18_enquiryfee                   <>0
or  avg_amount18_enquiryfee                   <>0
or  max_amount18_enquiryfee                   <>0
or  sum_cnt18_enquiryfee                      <>0
or  min_amount18_enquiryfee                   <>0
or  sum_amount15_enquiryfee                   <>0
or  avg_amount15_enquiryfee                   <>0
or  max_amount15_enquiryfee                   <>0
or  sum_cnt15_enquiryfee                      <>0
or  min_amount15_enquiryfee                   <>0
or  sum_amount19_enquiryfee                   <>0
or  avg_amount19_enquiryfee                   <>0
or  max_amount19_enquiryfee                   <>0
or  sum_cnt19_enquiryfee                      <>0
or  min_amount19_enquiryfee                   <>0
or  sum_amount20_enquiryfee                   <>0
or  avg_amount20_enquiryfee                   <>0
or  max_amount20_enquiryfee                   <>0
or  sum_cnt20_enquiryfee                      <>0
or  min_amount20_enquiryfee                   <>0
or  sum_amount14_enquiryfee                   <>0
or  avg_amount14_enquiryfee                   <>0
or  max_amount14_enquiryfee                   <>0
or  sum_cnt14_enquiryfee                      <>0
or  min_amount14_enquiryfee                   <>0
or  sum_amount21_enquiryfee                   <>0
or  avg_amount21_enquiryfee                   <>0
or  max_amount21_enquiryfee                   <>0
or  sum_cnt21_enquiryfee                      <>0
or  min_amount21_enquiryfee                   <>0
or  sum_amount13_enquiryfee                   <>0
or  avg_amount13_enquiryfee                   <>0
or  max_amount13_enquiryfee                   <>0
or  sum_cnt13_enquiryfee                      <>0
or  min_amount13_enquiryfee                   <>0
or  sum_amount22_enquiryfee                   <>0
or  avg_amount22_enquiryfee                   <>0
or  max_amount22_enquiryfee                   <>0
or  sum_cnt22_enquiryfee                      <>0
or  min_amount22_enquiryfee                   <>0
or  sum_amount12_enquiryfee                   <>0
or  avg_amount12_enquiryfee                   <>0
or  max_amount12_enquiryfee                   <>0
or  min_amount12_enquiryfee                   <>0
or  sum_cnt12_enquiryfee                      <>0
or  sum_amount23_enquiryfee                   <>0
or  avg_amount23_enquiryfee                   <>0
or  max_amount23_enquiryfee                   <>0
or  sum_cnt23_enquiryfee                      <>0
or  min_amount23_enquiryfee                   <>0
or  sum_amount11_enquiryfee                   <>0
or  avg_amount11_enquiryfee                   <>0
or  max_amount11_enquiryfee                   <>0
or  min_amount11_enquiryfee                   <>0
or  sum_cnt11_enquiryfee                      <>0
or  sum_amount24_enquiryfee                   <>0
or  avg_amount24_enquiryfee                   <>0
or  max_amount24_enquiryfee                   <>0
or  sum_cnt24_enquiryfee                      <>0
or  min_amount24_enquiryfee                   <>0
or  sum_amount10_enquiryfee                   <>0
or  avg_amount10_enquiryfee                   <>0
or  max_amount10_enquiryfee                   <>0
or  sum_cnt10_enquiryfee                      <>0
or  min_amount10_enquiryfee                   <>0
or  sum_amount9_enquiryfee                    <>0
or  avg_amount9_enquiryfee                    <>0
or  max_amount9_enquiryfee                    <>0
or  sum_cnt9_enquiryfee                       <>0
or  min_amount9_enquiryfee                    <>0
or  sum_amount25_enquiryfee                   <>0
or  avg_amount25_enquiryfee                   <>0
or  max_amount25_enquiryfee                   <>0
or  sum_cnt25_enquiryfee                      <>0
or  min_amount25_enquiryfee                   <>0
or  sum_amount8_enquiryfee                    <>0
or  avg_amount8_enquiryfee                    <>0
or  max_amount8_enquiryfee                    <>0
or  sum_cnt8_enquiryfee                       <>0
or  min_amount8_enquiryfee                    <>0
or  sum_amount26_enquiryfee                   <>0
or  avg_amount26_enquiryfee                   <>0
or  max_amount26_enquiryfee                   <>0
or  sum_cnt26_enquiryfee                      <>0
or  min_amount26_enquiryfee                   <>0
or  sum_amount7_enquiryfee                    <>0
or  avg_amount7_enquiryfee                    <>0
or  max_amount7_enquiryfee                    <>0
or  sum_cnt7_enquiryfee                       <>0
or  min_amount7_enquiryfee                    <>0
or  sum_amount27_enquiryfee                   <>0
or  avg_amount27_enquiryfee                   <>0
or  max_amount27_enquiryfee                   <>0
or  sum_cnt27_enquiryfee                      <>0
or  min_amount27_enquiryfee                   <>0
or  sum_amount6_enquiryfee                    <>0
or  avg_amount6_enquiryfee                    <>0
or  max_amount6_enquiryfee                    <>0
or  sum_cnt6_enquiryfee                       <>0
or  min_amount6_enquiryfee                    <>0
or  sum_amount28_enquiryfee                   <>0
or  avg_amount28_enquiryfee                   <>0
or  max_amount28_enquiryfee                   <>0
or  sum_cnt28_enquiryfee                      <>0
or  min_amount28_enquiryfee                   <>0
or  sum_amount5_enquiryfee                    <>0
or  avg_amount5_enquiryfee                    <>0
or  max_amount5_enquiryfee                    <>0
or  sum_cnt5_enquiryfee                       <>0
or  min_amount5_enquiryfee                    <>0
or  sum_amount4_enquiryfee                    <>0
or  avg_amount4_enquiryfee                    <>0
or  max_amount4_enquiryfee                    <>0
or  sum_cnt4_enquiryfee                       <>0
or  min_amount4_enquiryfee                    <>0
or  sum_amount29_enquiryfee                   <>0
or  avg_amount29_enquiryfee                   <>0
or  max_amount29_enquiryfee                   <>0
or  sum_cnt29_enquiryfee                      <>0
or  min_amount29_enquiryfee                   <>0
or  sum_amount3_enquiryfee                    <>0
or  avg_amount3_enquiryfee                    <>0
or  max_amount3_enquiryfee                    <>0
or  min_amount3_enquiryfee                    <>0
or  sum_cnt3_enquiryfee                       <>0
or  sum_amount30_enquiryfee                   <>0
or  avg_amount30_enquiryfee                   <>0
or  max_amount30_enquiryfee                   <>0
or  sum_cnt30_enquiryfee                      <>0
or  min_amount30_enquiryfee                   <>0
or  sum_amount2_enquiryfee                    <>0
or  avg_amount2_enquiryfee                    <>0
or  max_amount2_enquiryfee                    <>0
or  sum_cnt2_enquiryfee                       <>0
or  min_amount2_enquiryfee                    <>0
or  sum_amount1_enquiryfee                    <>0
or  avg_amount1_enquiryfee                    <>0
or  max_amount1_enquiryfee                    <>0
or  sum_cnt1_enquiryfee                       <>0
or  min_amount1_enquiryfee                    <>0
or  sum_amount31_enquiryfee                   <>0
or  avg_amount31_enquiryfee                   <>0
or  max_amount31_enquiryfee                   <>0
or  sum_cnt31_enquiryfee                      <>0
or  min_amount31_enquiryfee                   <>0
or  sum_amount32_enquiryfee                   <>0
or  avg_amount32_enquiryfee                   <>0
or  max_amount32_enquiryfee                   <>0
or  sum_cnt32_enquiryfee                      <>0
or  min_amount32_enquiryfee                   <>0
or  sum_amount33_enquiryfee                   <>0
or  avg_amount33_enquiryfee                   <>0
or  max_amount33_enquiryfee                   <>0
or  sum_cnt33_enquiryfee                      <>0
or  min_amount33_enquiryfee                   <>0
or  sum_amount34_enquiryfee                   <>0
or  avg_amount34_enquiryfee                   <>0
or  max_amount34_enquiryfee                   <>0
or  sum_cnt34_enquiryfee                      <>0
or   min_amount34_enquiryfee                <> 0   
or   sum_amount35_enquiryfee                <> 0   
or   avg_amount35_enquiryfee                <> 0   
or   max_amount35_enquiryfee                <> 0   
or   sum_cnt35_enquiryfee                   <> 0   
or   min_amount35_enquiryfee                <> 0   
or   sum_amount36_enquiryfee                <> 0   
or   avg_amount36_enquiryfee                <> 0   
or   max_amount36_enquiryfee                <> 0   
or   sum_cnt36_enquiryfee                   <> 0   
or   min_amount36_enquiryfee                <> 0   
or   sum_amount37_enquiryfee                <> 0   
or   avg_amount37_enquiryfee                <> 0   
or   max_amount37_enquiryfee                <> 0   
or   sum_cnt37_enquiryfee                   <> 0   
or   min_amount37_enquiryfee                <> 0   
or   sum_amount38_enquiryfee                <> 0   
or   avg_amount38_enquiryfee                <> 0   
or   max_amount38_enquiryfee                <> 0   
or   sum_cnt38_enquiryfee                   <> 0   
or   min_amount38_enquiryfee                <> 0   
or   sum_amount39_enquiryfee                <> 0   
or   avg_amount39_enquiryfee                <> 0   
or   max_amount39_enquiryfee                <> 0   
or   sum_cnt39_enquiryfee                   <> 0   
or   min_amount39_enquiryfee                <> 0   
or   sum_amount40_enquiryfee                <> 0   
or   avg_amount40_enquiryfee                <> 0   
or   max_amount40_enquiryfee                <> 0   
or   min_amount40_enquiryfee                <> 0   
or   sum_cnt40_enquiryfee                   <> 0   
or   sum_amount41_enquiryfee                <> 0   
or   avg_amount41_enquiryfee                <> 0   
or   max_amount41_enquiryfee                <> 0   
or   sum_cnt41_enquiryfee                   <> 0   
or   min_amount41_enquiryfee                <> 0   
or   sum_amount42_enquiryfee                <> 0   
or   avg_amount42_enquiryfee                <> 0   
or   max_amount42_enquiryfee                <> 0   
or   sum_cnt42_enquiryfee                   <> 0   
or   min_amount42_enquiryfee <>0 )
;



select * from phone_variable_tnh_creditcardrepayments_train_tc_result_new limit 10;
select * from phone_variable_yfq_creditcardrepayments_train_tc_result_new limit 10;                                                                                      