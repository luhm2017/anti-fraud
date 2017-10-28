package com.lakala.finance.anti_fraud.biz

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.lakala.finance.anti_fraud.common.DateUtils
import com.lakala.finance.anti_fraud.conf.Graph2VarConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/8/8.
  */
class Graph2Var(config: Graph2VarConfig, hc: HiveContext, readWriteLock: ReentrantReadWriteLock,
                orderQ: LinkedBlockingQueue[String],
                oneDegreeQ: LinkedBlockingQueue[ArrayBuffer[String]],
                twoDegreeQ: LinkedBlockingQueue[ArrayBuffer[String]]) extends Runnable with Logging {

  val jdbcUrl = s"${config.mysql_url}?user=${config.mysql_user}&password=${config.mysql_passwd}&useUnicode=true&characterEncoding=utf-8&useSSL=false&autoReconnect=true&failOverReadOnly=false"
  logWarning(s"mysql连接串:${jdbcUrl}")

  override def run(): Unit = {
    hc.sql("use lkl_card_score")
    //设置ck地址提升重算的稳定性
    hc.sparkContext.setCheckpointDir("hdfs://ns1/spark/checkpoint/Graph2Var")

    //刷新对应的行为数据
    flashPerformance()

    while (true) {
      //按照配置睡眠一定时间后再开始计算
//      Thread.sleep(config.comput_interval_time)

      val start_time: Long = System.currentTimeMillis()
      var oneDegreeArr : ArrayBuffer[(String, String, String, String, String, String, String)] = null
      var twoDegreeArr : ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String)]= null
      var orderArr: ArrayBuffer[String] = null
      logInfo(s"${Thread.currentThread().getName}线程开始申请写锁")
      readWriteLock.writeLock().lock()
      logInfo(s"${Thread.currentThread().getName}成功获取写锁")
      val computeFlage : Boolean = oneDegreeQ.size() > 0
      try {
        if (computeFlage) {
          //一度关联的数据
          oneDegreeArr = new ArrayBuffer[(String, String, String, String, String, String, String)]()
          for (i <- 0 until oneDegreeQ.size()) {
            val arrayBuf: ArrayBuffer[String] = oneDegreeQ.poll()
            oneDegreeArr.+=((arrayBuf(0), arrayBuf(1), arrayBuf(2), arrayBuf(3), arrayBuf(4),
              arrayBuf(5), arrayBuf(6)))
          }
          //二度关联的数据
          twoDegreeArr = new ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String)]()
          for (i <- 0 until twoDegreeQ.size()) {
            val arrayBuf = twoDegreeQ.poll()
            twoDegreeArr.+=((arrayBuf(0), arrayBuf(1), arrayBuf(2), arrayBuf(3), arrayBuf(4), arrayBuf(5), arrayBuf(6),
              arrayBuf(7), arrayBuf(8), arrayBuf(9), arrayBuf(10)))
          }
          //对应的订单数量
          orderArr = new ArrayBuffer[String]()
          for (i <- 0 until orderQ.size()) {
            orderArr.+=(orderQ.poll())
          }
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      } finally {
        readWriteLock.writeLock().unlock()
        logInfo(s"${Thread.currentThread().getName}释放写锁")
      }

      try{
        if(computeFlage){
          //刷新对应的行为数据
          flashPerformance()
          logWarning(s"${Thread.currentThread().getName}开始计算变量,1度数据总量:${oneDegreeArr.length},2度数据总量:${twoDegreeArr.length},总共订单数:${orderArr.size},订单号:${orderArr}")
          val sc: SparkContext = hc.sparkContext
          import hc.implicits._
          val oneDegreeDF: DataFrame = sc.makeRDD(oneDegreeArr).toDF("c0", "c1", "c2", "c3", "c4", "c5", "c6")
          val twoDegreeDF: DataFrame = sc.makeRDD(twoDegreeArr).toDF("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10")
          val orderDF: DataFrame = sc.makeRDD(orderArr).toDF("c0")
          oneDegreeDF.registerTempTable("one_degree_data_temp")
          twoDegreeDF.registerTempTable("two_degree_data_temp")
          orderDF.registerTempTable("graphx_tansported_ordernos_inc")

          var fqz_order_data_inc_df = hc.sql("select\n'1' as degree_type,\na.c0 as order_src,\n--a.c00 as cert_no_src,\na.c6 as cert_no_src,\n--a.c01 as apply_time_src,\na.c5 as apply_time_src,\n--b.performance as performance_src,\n--b.apply_time as apply_time_src,\n--b.type as type_src,\n--b.history_due_day as history_due_day_src,\n--b.current_due_day as current_due_day_src,\n--b.cert_no as cert_no_src,\n--b.label as label_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\nc.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\nc.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\n'null' as c5,\n'null' as c6,\n'null' as c7,\n'null' as order2,\n'null' as performance2,\n'null' as apply_time2,\n'null' as type2,\n0 as history_due_day2,\n0 as current_due_day2,\n'null' as cert_no2,\n0 as label2\nfrom one_degree_data_temp a   --实时进件一度关联图\n--join fqz_order_performance_data_new b on a.c0 = b.order_id\njoin fqz_order_performance_data_new_temp c on a.c4 = c.order_id\njoin graphx_tansported_ordernos_inc d on a.c0 = d.c0\nunion all\nselect \n'2' as degree_type,\na.c0 as order_src,\na.c10 as cert_no_src,\na.c9 as apply_time_src,\n--b.performance as performance_src,\n--b.apply_time as apply_time_src,\n--b.type as type_src,\n--b.history_due_day as history_due_day_src,\n--b.current_due_day as current_due_day_src,\n--b.cert_no as cert_no_src,\n--b.label as label_src,\na.c1,a.c2,a.c3,\na.c4 as order1,\nc.performance as performance1,\nc.apply_time as apply_time1,\nc.type as type1,\nc.history_due_day as history_due_day1,\nc.current_due_day as current_due_day1,\nc.cert_no as cert_no1,\nc.label as label1,\na.c5,a.c6,a.c7,\na.c8 as order2,\nd.performance as performance2,\nd.apply_time as apply_time2,\nd.type as type2,\nd.history_due_day as history_due_day2,\nd.current_due_day as current_due_day2,\nd.cert_no as cert_no2,\nd.label as label2\nfrom two_degree_data_temp a  --实时进件二度关联图\n--join fqz_order_performance_data_new b on a.c0 = b.order_id\njoin fqz_order_performance_data_new_temp c on a.c4 = c.order_id\njoin fqz_order_performance_data_new_temp d on a.c8 = d.order_id\njoin graphx_tansported_ordernos_inc e on a.c0 = e.c0")
          if (config.joined_partiontion_num != 0) {
            fqz_order_data_inc_df = fqz_order_data_inc_df.repartition(config.joined_partiontion_num)
          }
          fqz_order_data_inc_df.registerTempTable("fqz_order_data_inc")
          hc.sql("cache table fqz_order_data_inc")

          hc.sql("-- 一度关联自身_订单数量     \nSELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \nFROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' \nand a.apply_time_src>a.apply_time1\nand a.cert_no_src = a.cert_no1\ngroup by  a.order_src    \nunion all \n-- 一度关联自身_ID数量  \nSELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' and a.apply_time_src>a.apply_time1\nand a.cert_no_src = a.cert_no1\ngroup by a.order_src \nunion all\n-- 一度关联自身_黑合同数量\nSELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' and a.apply_time_src>a.apply_time1\nand a.cert_no_src = a.cert_no1 and a.label1 = 1\ngroup by a.order_src\n-- 一度关联自身_Q标拒绝数量  1  \nunion all \nSELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现  \nwhere performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\nand a.cert_no_src = a.cert_no1\ngroup by a.order_src \nunion all\n-- 一度关联自身_通过合同数量 \nSELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1\nand a.cert_no_src = a.cert_no1\ngroup by a.order_src ")
            .registerTempTable("overdue_cnt_self_instant")

          hc.sql("select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt_self ,           \nsum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt_self ,   \nsum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt_self , \nsum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt_self,    \nsum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt_self \nfrom  overdue_cnt_self_instant\ngroup by order_src")
            .registerTempTable("overdue_cnt_self_sum_instant")

          hc.sql("select c.order_src,\n        'overdue0' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1<=0  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n   group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1>3  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n  group by c.order_src \nunion all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n   and c.cert_no_src = c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>3 --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联自身\n   and c.cert_no_src = c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>30  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联自身\n   and c.cert_no_src = c.cert_no1\n   group by c.order_src")
            .registerTempTable("overdue_cnt_2_self_tmp_instant")

          hc.sql("select order_src,\nsum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \nsum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \nsum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \nsum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \nsum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\nsum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \nfrom  overdue_cnt_2_self_tmp_instant\ngroup by order_src")
            .registerTempTable("overdue_cnt_2_self_instant")

          hc.sql("-- 一度_订单数量  4  \nSELECT a.order_src,'order_cnt' title, \ncount(distinct a.order1) cnt \nFROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' \nand a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by  a.order_src    \nunion all \n-- 一度_ID数量    \nSELECT a.order_src,'id_cnt' title,count(distinct a.cert_no1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by a.order_src \nunion all\n-- 一度关联自身_黑合同数量\nSELECT a.order_src,'black_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='1' and a.apply_time_src>a.apply_time1\nand a.cert_no_src <> a.cert_no1 and a.label1 = 1\ngroup by a.order_src\n-- 一度_Q标拒绝数量  1  \nunion all \nSELECT a.order_src,'q_refuse_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现  \nwhere performance1='q_refuse' and a.degree_type='1' and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by a.order_src \nunion all\n-- 一度_通过合同数量  2  \nSELECT a.order_src,'pass_cnt' title,count(distinct a.order1) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.type1='pass'  and a.degree_type='1' and a.apply_time_src>a.apply_time1\n--一度关联排除自身\nand a.cert_no_src <> a.cert_no1\ngroup by a.order_src")
            .registerTempTable("overdue_cnt_1_instant")

          hc.sql("select order_src,\nsum(case when title= 'order_cnt' then cnt else 0 end ) order_cnt ,           \nsum(case when title= 'id_cnt' then cnt else 0 end ) id_cnt ,\nsum(case when title= 'black_cnt' then cnt else 0 end ) black_cnt ,    \nsum(case when title= 'q_refuse_cnt' then cnt else 0 end ) q_refuse_cnt ,    \nsum(case when title= 'pass_cnt' then cnt else 0 end ) pass_cnt \nfrom  overdue_cnt_1_instant\ngroup by order_src")
            .registerTempTable("overdue_cnt_1_sum_instant")

          hc.sql("select c.order_src,\n        'overdue0' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1<=0  --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n   group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1>3 --当前 \n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.current_due_day1>30 --当前\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n  group by c.order_src \nunion all\n--历史逾期\nselect c.order_src,\n        'overdue0_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1<=0  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>3 --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1\n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1   \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30_ls' title\n       ,count(distinct c.order1) cnt \n       from \n   fqz_order_data_inc c \n where c.type1='pass'       --通过 \n   and c.history_due_day1>30  --历史\n   and c.degree_type='1' and c.apply_time_src>c.apply_time1  \n   --一度关联排除自身\n   and c.cert_no_src <> c.cert_no1\n   group by c.order_src")
            .registerTempTable("overdue_cnt_2_tmp_instant")

          hc.sql("select order_src,\nsum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \nsum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \nsum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \nsum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \nsum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\nsum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \nfrom  overdue_cnt_2_tmp_instant\ngroup by order_src")
            .registerTempTable("overdue_cnt_2_instant")

          hc.sql("-- 2度_订单数量     \nSELECT a.order_src,'order_cnt' title, \ncount(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \nand a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\nand a.apply_time_src>a.apply_time1\ngroup by  a.order_src    \nunion all \n-- 2度_ID数量     \nSELECT a.order_src,'id_cnt' title,\ncount(distinct a.cert_no2) cnt \nFROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \nand a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\nand a.apply_time_src>a.apply_time1\ngroup by a.order_src \nunion all\n-- 2度_黑合同数量\nSELECT a.order_src,'black_cnt' title,count(distinct a.order2) cnt FROM fqz_order_data_inc a     --order订单表现\nwhere a.degree_type='2' \nand a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \nand a.apply_time_src>a.apply_time1\nand a.label2 = 1\ngroup by a.order_src\n-- 2度_Q标拒绝数量  2  \nunion all \nSELECT a.order_src,'q_refuse_cnt' title,\ncount(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现  \nwhere performance2='q_refuse' \nand a.degree_type='2' \nand a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前 \nand a.apply_time_src>a.apply_time1\ngroup by a.order_src \nunion all\n-- 2度_通过合同数量  2  \nSELECT a.order_src,'pass_cnt' title,\ncount(distinct a.order2) cnt \nFROM fqz_order_data_inc a     --order订单表现\nwhere a.type2='pass'  \nand a.degree_type='2' \nand a.apply_time_src>a.apply_time2 --关图的时间都必须在当前进件时间之前\nand a.apply_time_src>a.apply_time1\ngroup by a.order_src")
            .registerTempTable("overdue_cnt_1_2_instant")


          hc.sql("select c.order_src,\n        'overdue0' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.current_due_day2<=0  --当前\n   and c.degree_type='2' \n   and c.apply_time_src>c.apply_time1  \n   and c.apply_time_src>c.apply_time2 \n   group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.current_due_day2>3 --当前 \n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.current_due_day2>30  --当前\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n   and c.apply_time_src>c.apply_time2 \n  group by c.order_src \nunion all\nselect c.order_src,\n        'overdue0_ls' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.history_due_day2<=0 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n   and c.apply_time_src>c.apply_time2    \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue3_ls' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.history_due_day2> 3 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1  \n   and c.apply_time_src>c.apply_time2 \n  group by c.order_src\nunion all\nselect c.order_src,\n        'overdue30_ls' title\n       ,count(distinct c.order2) cnt \n       from \n   fqz_order_data_inc c \n where c.type2='pass'       --通过 \n   and c.history_due_day2> 30 --历史\n   and c.degree_type='2' and c.apply_time_src>c.apply_time1 \n   and c.apply_time_src>c.apply_time2    \n   group by c.order_src")
            .registerTempTable("overdue_cnt_2_2_tmp_instant")

          hc.sql("select order_src,\nsum(case when title= 'overdue0' then cnt else 0 end ) overdue0 ,           \nsum(case when title= 'overdue3' then cnt else 0 end ) overdue3 ,   \nsum(case when title= 'overdue30' then cnt else 0 end ) overdue30 ,  \nsum(case when title= 'overdue0_ls' then cnt else 0 end ) overdue0_ls, \nsum(case when title= 'overdue3_ls' then cnt else 0 end ) overdue3_ls  ,\nsum(case when title= 'overdue30_ls' then cnt else 0 end ) overdue30_ls \nfrom  overdue_cnt_2_2_tmp_instant\ngroup by order_src")
            .registerTempTable("overdue_cnt_2_2_instant")

          hc.sql("select trim(order_src) order_src   \nfrom fqz_order_data_inc\ngroup by order_src")
            .registerTempTable("order_src_group_instant")

          hc.sql("select \na.order_src,\nnvl(b.order_cnt_self,0) as order_cnt_self,\nnvl(b.id_cnt_self,0) as id_cnt_self,\nnvl(b.black_cnt_self,0) as black_cnt_self,\nnvl(b.q_refuse_cnt_self,0) as q_refuse_cnt_self,\nnvl(b.pass_cnt_self,0) as pass_cnt_self,\nnvl(c.overdue0,0) as overdue0_self,\nnvl(c.overdue3,0) as overdue3_self,\nnvl(c.overdue30,0) as overdue30_self,\nnvl(c.overdue0_ls,0) as overdue0_ls_self,\nnvl(c.overdue3_ls,0) as overdue3_ls_self,\nnvl(c.overdue30_ls,0) as overdue30_ls_self,\nnvl(d.order_cnt,0) as order_cnt,\nnvl(d.id_cnt,0) as id_cnt,\nnvl(d.black_cnt,0) as black_cnt,\nnvl(d.q_refuse_cnt,0) as q_refuse_cnt,\nnvl(d.pass_cnt,0) as pass_cnt,\nnvl(e.overdue0,0) as overdue0,\nnvl(e.overdue3,0) as overdue3,\nnvl(e.overdue30,0) as overdue30,\nnvl(e.overdue0_ls,0) as overdue0_ls ,\nnvl(e.overdue3_ls,0) as overdue3_ls,\nnvl(e.overdue30_ls,0) as overdue30_ls, \nnvl(f.order_cnt,0) as order_cnt_2,\nnvl(f.id_cnt,0)       as id_cnt_2,\nnvl(f.black_cnt,0)       as black_cnt_2,\nnvl(f.q_refuse_cnt,0) as q_refuse_cnt_2,\nnvl(f.pass_cnt,0)    as pass_cnt_2,\nnvl(g.overdue0,0)    as overdue0_2,\nnvl(g.overdue3,0)    as overdue3_2,\nnvl(g.overdue30,0)   as overdue30_2,\nnvl(g.overdue0_ls,0) as overdue0_ls_2,\nnvl(g.overdue3_ls,0) as overdue3_ls_2,\nnvl(g.overdue30_ls,0) as overdue30_ls_2\nfrom order_src_group_instant a   --256441\nleft join overdue_cnt_self_sum_instant b on a.order_src = b.order_src  -- 一度关联自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\nleft join overdue_cnt_2_self_instant c on a.order_src = c.order_src    -- 合并一度关联 逾期数据(关联自身)\nleft join overdue_cnt_1_sum_instant d on a.order_src = d.order_src     -- 一度关联排除自身（订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\nleft join overdue_cnt_2_instant  e on a.order_src = e.order_src        -- 一度关联 逾期数据(排除自身)\nleft join overdue_cnt_2_sum_instant f on a.order_src = f.order_src     -- 二度关联数据 （订单数量、ID数量、黑合同数量、Q标拒绝数量 ）\nleft join overdue_cnt_2_2_instant g on a.order_src = g.order_src")
            .registerTempTable("overdue_result_all_instant")

          hc.sql("select * from overdue_result_all_instant\nwhere !(\n  order_cnt_self         = 0 and \n  id_cnt_self           = 0 and \n  black_cnt_self       = 0 and\n  q_refuse_cnt_self     = 0 and \n  pass_cnt_self         = 0 and \n  overdue0_self         = 0 and \n  overdue3_self         = 0 and \n  overdue30_self         = 0 and \n  overdue0_ls_self      = 0 and \n  overdue3_ls_self      = 0 and \n  overdue30_ls_self     = 0 and \n  order_cnt           = 0 and \n  id_cnt               = 0 and \n  black_cnt            = 0 and\n  q_refuse_cnt         = 0 and \n  pass_cnt           = 0 and \n  overdue0           = 0 and \n  overdue3           = 0 and \n  overdue30           = 0 and \n  overdue0_ls           = 0 and \n  overdue3_ls           = 0 and \n  overdue30_ls         = 0 and \n  order_cnt_2           = 0 and \n  id_cnt_2           = 0 and\n  black_cnt_2            = 0 and \n  q_refuse_cnt_2         = 0 and \n  pass_cnt_2           = 0 and \n  overdue0_2           = 0 and \n  overdue3_2           = 0 and \n  overdue30_2           = 0 and \n  overdue0_ls_2         = 0 and \n  overdue3_ls_2         = 0 and \n  overdue30_ls_2         = 0\n)")
            .registerTempTable("overdue_result_all_instant")


          hc.sql("select \na.order_src,\nconcat(a.c1,'|',a.c3) as ljmx,\n1 as depth \nfrom fqz_order_data_inc a\nwhere a.degree_type = '1' \nand a.apply_time_src>a.apply_time1 \n--一度关联进件为黑\nand a.label1 = 1 \nunion all \nselect \na.order_src,\nconcat(a.c1,'|',a.c3,'|',a.c5,'|',a.c7) as ljmx,\n2 as depth \nfrom fqz_order_data_inc a\nwhere a.degree_type = '2' \nand a.apply_time_src>a.apply_time1 \nand a.apply_time_src>a.apply_time2    \n--二度关联进件为黑\nand a.label2 = 1")
            .registerTempTable("order_src_bian_tmp_instant")

          hc.sql("select c.order_src,concat_ws(',',collect_set(ljmx)) as ljmx           \nfrom  order_src_bian_tmp_instant  c\ngroup by c.order_src")
            .registerTempTable("order_src_bian_instant")

          hc.sql("select \n--c.label,\ntab.c5 as apply_time,\na.*,b.ljmx\nfrom overdue_result_all_instant a\nleft join order_src_bian_instant b on a.order_src=b.order_src\njoin \none_degree_data_temp tab on a.order_src = tab.c0")
            .registerTempTable("overdue_result_all_new_instant")

          hc.sql("select order_src,max(depth) as depth from order_src_bian_tmp_instant \ngroup by order_src")
            .registerTempTable("fqz_edge_depth_instant")

          hc.sql("select \na.order_src,\nsum(b.woe) as edge_woe_sum,\nmax(woe) as edge_woe_max,\nmin(woe) as edge_woe_min\nfrom \nfqz_edge_data_total_instant a \njoin fqz_edge_woe b on a.edge = b.edge   --每个边woe值依赖全局统计\ngroup by a.order_src")
            .registerTempTable("fqz_order_edge_woe_instant")

          val cacheDF = hc.sql("select a.*,\nb.edge_woe_sum,\nb.edge_woe_max,\nb.edge_woe_min,\nc.depth\nfrom overdue_result_all_new_instant a \nleft join fqz_order_edge_woe_instant b on a.order_src = b.order_src\nleft join fqz_edge_depth_instant c on a.order_src = c.order_src")
            .cache()

          logInfo(s"${Thread.currentThread().getName}开始向hive中写入结果")
          cacheDF.write.mode(SaveMode.Append).saveAsTable("fqz_score_variable_new_2var")
          logInfo(s"${Thread.currentThread().getName}完成向hive中写入结果")
          logInfo(s"${Thread.currentThread().getName}开始往mysql中写入数据")
          cacheDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "fqz_score_variable_new1_2var", new Properties())
          logInfo(s"${Thread.currentThread().getName}已经完成向mysql中写入数据")

          cacheDF.unpersist()
          hc.sql("uncache table fqz_order_data_inc")
          logWarning(s"${Thread.currentThread().getName}完成计算变量，耗时:${(System.currentTimeMillis() - start_time) / 1000.0}s")
        }
      }catch {
        case ex: Exception =>
          ex.printStackTrace()
      }

    }
  }

  private var getDataTime: Long = 0

  //按照定时策略刷新对应的行为数据表
  private def flashPerformance() = {
    val updateInterval = System.currentTimeMillis() - getDataTime
    if (getDataTime == 0 && updateInterval > config.performance_update_time) {
      logWarning(s"开始刷新行为表的数据")
      val (year, month, day) = DateUtils.getTodayInfo()
      var performDF1: DataFrame = hc.sql(s"select * from fqz_order_performance_data_new where year=${year} and month=${month} and day=${day}")
      if (config.performance_partintion_num != 0) {
        performDF1 = performDF1.repartition(config.performance_partintion_num)
      }
      performDF1.registerTempTable("fqz_order_performance_data_new_temp")
      hc.sql("cache table fqz_order_performance_data_new_temp")
      getDataTime = System.currentTimeMillis()
      logWarning(s"行为表数据刷新完成")
    }
  }

}
