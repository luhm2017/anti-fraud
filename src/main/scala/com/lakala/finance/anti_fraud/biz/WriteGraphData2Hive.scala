package com.lakala.finance.anti_fraud.biz

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.lakala.finance.anti_fraud.common.{CommonUtil, DateUtils}
import com.lakala.finance.anti_fraud.conf.Neo2HiveConfig
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/8/8.
  */
class WriteGraphData2Hive(config: Neo2HiveConfig, readWriteLock: ReentrantReadWriteLock, sqlContext: SQLContext,
                          degree1Queue: LinkedBlockingQueue[ArrayBuffer[String]],
                          degree2Queue: LinkedBlockingQueue[ArrayBuffer[String]],
                          ordernoQueue: LinkedBlockingQueue[(String, String, Int, Int, Int)]) extends Runnable with Logging {
  override def run(): Unit = {
    logInfo(s"${Thread.currentThread().getName}写数据任务开始执行。。。。")
    sqlContext.sql("use lkl_card_score")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    while (true) {
      Thread.sleep(config.write_batch_time * 1000)
      val (year, mouth, day) = DateUtils.getTodayInfo()
      logInfo(s"${Thread.currentThread().getName}线程开始申请写锁")
      readWriteLock.writeLock().lock()
      logInfo(s"${Thread.currentThread().getName}成功获取写锁")
      try {
        val start_time: Long = System.currentTimeMillis()
        logInfo(s"${Thread.currentThread().getName}开始执行导出任务:一度关联数据量:${degree1Queue.size()},二度关联数据量:${degree2Queue.size()}")
        //提交一度关联
        val degree1arr = new ArrayBuffer[(String, String, String, String, String, String, String, Int, Int, Int)]()
        for (i <- 0 until degree1Queue.size()) {
          val arrayBuf: ArrayBuffer[String] = degree1Queue.poll()
          degree1arr.+=((arrayBuf(0), arrayBuf(1), arrayBuf(2), arrayBuf(3), arrayBuf(4),
            arrayBuf(5), arrayBuf(6), year, mouth, day))
        }
        if (degree1arr.size > 0) {
          logInfo(s"${Thread.currentThread().getName}开始提交一度关联的数据,数据量:${degree1arr.size}")
          val start: Long = System.currentTimeMillis()
          val degree1Rdd = sqlContext.sparkContext.makeRDD(degree1arr)
          import sqlContext.implicits._
          val degree1DF: DataFrame = degree1Rdd.toDF("c0", "c1", "c2", "c3", "c4", "c5", "c6", "year", "month", "day").repartition(3)
          degree1DF.write.mode(SaveMode.Append).partitionBy("year", "month", "day").saveAsTable(config.degree1TableName)
          degree1DF.unpersist()
          logInfo(s"${Thread.currentThread().getName}完一度度关联的数据提交,耗时${(System.currentTimeMillis() - start) / 1000.0}s")
        }

        //提交二度关联
        val degree2arr = new ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, Int, Int, Int)]()
        for (i <- 0 until degree2Queue.size()) {
          val arrayBuf = degree2Queue.poll()
          degree2arr.+=((arrayBuf(0), arrayBuf(1), arrayBuf(2), arrayBuf(3), arrayBuf(4), arrayBuf(5),
            arrayBuf(6), arrayBuf(7), arrayBuf(8), arrayBuf(9), arrayBuf(10), year, mouth, day))
        }
        if (degree2arr.size > 0) {
          logInfo(s"${Thread.currentThread().getName}开始提交二度关联的数据,数据量${degree2arr.size}")
          val start: Long = System.currentTimeMillis()
          val degree2Rdd = sqlContext.sparkContext.makeRDD(degree2arr)
          import sqlContext.implicits._
          val degree2DF: DataFrame = degree2Rdd.toDF("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "year", "month", "day").repartition(3)
          degree2DF.write.mode(SaveMode.Append).partitionBy("year", "month", "day").saveAsTable(config.degree2TableName)
          degree2DF.unpersist()
          logInfo(s"${Thread.currentThread().getName}完二度度度关联的数据提交,耗时${(System.currentTimeMillis() - start) / 1000.0}s")
        }

        // 将订单号写入到hive库中
        val ordernoArr = new ArrayBuffer[(String, String, Int, Int, Int)]()
        for (i <- 0 until ordernoQueue.size()) {
          ordernoArr.+=(ordernoQueue.poll())
        }
        if (ordernoArr.size > 0) {
          logInfo(s"${Thread.currentThread().getName},共${ordernoArr.size}个订单，完成提交了${ordernoArr.map(x => x._1)},现在将对应的时间数据写入hive中")
          val start: Long = System.currentTimeMillis()
          val ordernoRdd: RDD[(String, String, Int, Int, Int)] = sqlContext.sparkContext.makeRDD(ordernoArr)
          import sqlContext.implicits._
          val ordernoDF: DataFrame = ordernoRdd.toDF("c0", "c1", "year", "month", "day")
          ordernoDF.write.mode(SaveMode.Append).partitionBy("year", "month", "day").saveAsTable("graphx_tansported_ordernos")
          ordernoDF.unpersist()
          logInfo(s"${Thread.currentThread().getName}完成提交对应的订单号，耗时${(System.currentTimeMillis() - start) / 1000.0}s")
        }

        logInfo(s"${Thread.currentThread().getName}导出完成,总共耗时:${(System.currentTimeMillis() - start_time) / 1000.0}s")
      } catch {
        case ex: Exception =>
          val info: String = CommonUtil.getExceptionInfo(ex)
          //          ex.printStackTrace()
          logError(s"${Thread.currentThread().getName}写hive线程出现异常:\n${info}")
      } finally {
        readWriteLock.writeLock().unlock()
        logInfo(s"${Thread.currentThread().getName}释放写锁")
      }
    }
  }
}
