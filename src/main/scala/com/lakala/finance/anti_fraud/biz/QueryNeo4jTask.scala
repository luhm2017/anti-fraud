package com.lakala.finance.anti_fraud.biz

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.lakala.finance.anti_fraud.common.{CommonUtil, DateUtils, SQL}
import com.lakala.finance.anti_fraud.conf.Neo2HiveConfig
import com.lakala.finance.anti_fraud.exceptions.QueryDataTooHuge
import org.apache.spark.Logging
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by longxiaolei on 2017/8/8.
  */
class QueryNeo4jTask(config: Neo2HiveConfig, mesgQueue: LinkedBlockingQueue[String], readWriteLock: ReentrantReadWriteLock,
                     degree1Queue: LinkedBlockingQueue[ArrayBuffer[String]],
                     degree2Queue: LinkedBlockingQueue[ArrayBuffer[String]],
                     ordernoQueue: LinkedBlockingQueue[(String, String, Int, Int, Int)]) extends Runnable with Logging {
  private val degree1Fields: mutable.LinkedHashMap[String, String] = QueryNeo4jTask.degree1Fields
  private val degree2Fields: mutable.LinkedHashMap[String, String] = QueryNeo4jTask.degree2Fields
  private val page_sie = config.page_sie
  private val threshold = config.threshold
  private val orderno_flag = "${orderno}"
  private val mysqlDS: DruidDataSource = config.mysqlDs

  override def run(): Unit = {
    var neo4j_con: Connection = null
    var mesg: String = null
    var orderno: String = null
    var inset_time: String = null
    try {
      val one_degree_sql = SQL.NEO4J.DEGREE1_PAGE
      val two_degree_sql = SQL.NEO4J.DEGREE2_PAGE
      //      val max_wait_time: Long = config.neo4j_max_wait
      neo4j_con = DriverManager.getConnection(s"jdbc:neo4j:bolt://${config.neo4j_url}", config.neo4j_user, config.neo4j_passwd)
      while (true) {
        breakable {
          try {
            mesg = mesgQueue.take()
            logInfo(s"${Thread.currentThread().getName} 成功获取消息:${mesg}")

            val jsonobj: JSONObject = JSON.parseObject(mesg)
            orderno = jsonobj.getString("orderno")
            inset_time = jsonobj.getString("insert_time")
            val (year, mouth, day) = DateUtils.getTodayInfo()
            val identification = jsonobj.getString("cert_no")

            if (isWite(orderno, neo4j_con)) {
              logWarning(s"${orderno} 白名单")
              writeExOrder2Mysql(orderno, inset_time, "c")
              break()
            }

            val degree1Sql: String = one_degree_sql.replace(orderno_flag, orderno)
            val degree1start: Long = System.currentTimeMillis()
            val one_degree_result: ArrayBuffer[ArrayBuffer[String]] = queryNeo4j_paging(degree1Sql, neo4j_con, degree1Fields, orderno, inset_time, identification, year, mouth, day)
            val degree1end: Long = System.currentTimeMillis()

            if (one_degree_result.size == 0) {
              logInfo(s"${orderno}为孤点数据")
              writeExOrder2Mysql(orderno, inset_time, "b")
              break()
            }

            val degree2Sql: String = two_degree_sql.replace(orderno_flag, orderno)
            val degree2start: Long = System.currentTimeMillis()
            val two_degree_result: ArrayBuffer[ArrayBuffer[String]] = queryNeo4j_paging(degree2Sql, neo4j_con, degree2Fields, orderno, inset_time, identification, year, mouth, day)
            val degree2end: Long = System.currentTimeMillis()

            logInfo(s"${Thread.currentThread().getName}:${orderno},一度关联的数据量:${one_degree_result.size},耗时${(degree1end - degree1start) / 1000.0}s,二度关联的数据量为:${two_degree_result.size},耗时:${(degree2end - degree2start) / 1000.0}s")
            results2Queues(one_degree_result, two_degree_result, orderno, inset_time, year, mouth, day)
          } catch {
            case ex: JSONException =>
              logError(s"JSON格式异常，导致解析失败,错误JSON为:${mesg}")
            case ex: QueryDataTooHuge =>
              logError(s"${orderno} 查询的结果超过容量限制")
              writeExOrder2Mysql(orderno, inset_time, "a")
            case ex: ServiceUnavailableException => //todo neo4j 连接异常
              CommonUtil.closeIO(neo4j_con)
              Thread.sleep(3000)
              neo4j_con = DriverManager.getConnection(s"jdbc:neo4j:bolt://${config.neo4j_url}", config.neo4j_user, config.neo4j_passwd)
            case ex: Exception =>
              val info: String = CommonUtil.getExceptionInfo(ex)
              logError(s"${Thread.currentThread().getName}查询任务出现异常:\n${info}")
          }
        }
      }
    } finally {
      CommonUtil.closeIO(neo4j_con)
    }
  }

  private def isWite(orderno: String, conn: Connection): Boolean = {
    var isWitePs: PreparedStatement = null
    try {
      val sql = SQL.NEO4J.IS_WITE.replace("${orderno}", orderno)
      isWitePs = conn.prepareStatement(sql)
      val rs: ResultSet = isWitePs.executeQuery()
      var num = 0
      while (rs.next()) {
        num += 1
      }
      if (num > 0) {
        true
      }
      false
    } finally {
      CommonUtil.closeIO(isWitePs)
    }
  }

  private def writeExOrder2Mysql(orderno: String, inset_time: String, ex_type: String): Unit = {
    var conn: DruidPooledConnection = null
    try {
      conn = mysqlDS.getConnection
      val ps: PreparedStatement = conn.prepareStatement(SQL.MYSQL.INSET_EXCEPTION)
      ps.setString(1, orderno)
      ps.setString(2, inset_time)
      ps.setString(3, ex_type)
      ps.execute()
    } finally {
      CommonUtil.closeIO(conn)
    }
  }

  private def queryNeo4j_paging(sql: String, con: Connection, fields: mutable.LinkedHashMap[String, String],
                                orderno: String, insert_time: String, identification: String, year: Int, month: Int, day: Int): ArrayBuffer[ArrayBuffer[String]] = {
    val result: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    //    val (year, mouth, day) = DateUtils.getTodayInfo()
    val ps: PreparedStatement = con.prepareStatement(sql)
    var total_nums = 0
    var start_index = 0
    //每个结果集的大小
    var result_num = page_sie
    var page_num = 0
    while (result_num == page_sie) {
      ps.setInt(1, start_index)
      ps.setInt(2, page_sie)
      page_num += 1

      if (total_nums > threshold) {
        throw new QueryDataTooHuge()
      }

      result_num = 0
      //      logInfo(s"${Thread.currentThread().getName}:${orderno},分页起点和终点:${start_index}-${page_sie}, ,sql:${sql}")
      val rs: ResultSet = ps.executeQuery()
      while (rs.next()) {
        result_num += 1
        total_nums += 1
        if (total_nums > threshold) {
          throw new QueryDataTooHuge()
        }

        var str: String = null
        var data = ArrayBuffer[String]()
        try {
          fields.foreach {
            case (key, value) =>
              str = rs.getString(key)
              if (value == null) {
                data.+=(str)
              } else {
                data.+=(JSON.parseObject(str).getString(value))
              }
          }
          data.+=(insert_time)
          data.+=(identification)
          data.+=(year.toString)
          data.+=(month.toString)
          data.+=(day.toString)
          result.+=(data)
        } catch {
          case ex: JSONException =>
            val info: String = CommonUtil.getExceptionInfo(ex)
            logError(info)
            logError(s"订单号${orderno} 查询出错误的json：${str},跳过该条数据")
        }
      }

      start_index += result_num
    }
    result
  }

  private def results2Queues(one_degree_result: ArrayBuffer[ArrayBuffer[String]],
                             two_degree_result: ArrayBuffer[ArrayBuffer[String]],
                             orderno: String, insert_time: String, year: Int, month: Int, day: Int): Unit = {
    logInfo(s"${Thread.currentThread().getName}线程开始申请读锁")
    readWriteLock.readLock().lock()
    logInfo(s"${Thread.currentThread().getName}线程成功获取读锁")
    try {
      result2Queue(one_degree_result, degree1Queue)
      result2Queue(two_degree_result, degree2Queue)
      ordernoQueue.put((orderno, insert_time, year: Int, month: Int, day: Int))
    } finally {
      readWriteLock.readLock().unlock()
      logInfo(s"${Thread.currentThread().getName}线程释放读锁")
    }
  }

  private def result2Queue(result: ArrayBuffer[ArrayBuffer[String]], queue: LinkedBlockingQueue[ArrayBuffer[String]]): Unit = {
    result.foreach(data => {
      queue.put(data)
    })
  }

}

object QueryNeo4jTask {
  private val degree1Fields = mutable.LinkedHashMap(
    "v1.orderno" -> null,
    "e1" -> "type",
    //    "v2.modelname" -> null,
    "v2.content" -> null,
    "e2" -> "type",
    "v3.orderno" -> null
  )

  private val degree2Fields = mutable.LinkedHashMap(
    "v1.orderno" -> null,
    "e1" -> "type",
    //    "v2.modelname" -> null,
    "v2.content" -> null,
    "e2" -> "type",
    "v3.orderno" -> null,
    "e3" -> "type",
    //    "v4.modelname" -> null,
    "v4.content" -> null,
    "e4" -> "type",
    "v5.orderno" -> null
  )
}
