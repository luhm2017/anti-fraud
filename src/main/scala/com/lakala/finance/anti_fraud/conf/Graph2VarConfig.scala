package com.lakala.finance.anti_fraud.conf

import java.io.InputStream
import java.util

import com.alibaba.druid.pool.DruidDataSource

import org.apache.spark.Logging
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.immutable.Seq
import scala.xml.{Elem, XML}

/**
  * Created by longxiaolei on 2017/8/8.
  */
class Graph2VarConfig(path: String) extends Logging {
  private var confXml: Elem = null
  if("jar".equals(path)){
    val is: InputStream = getClass.getClassLoader.getResourceAsStream("Graph2VarConfig.xml")
    confXml =XML.load(is)
  }else{
    confXml =XML.load(path)
  }


  logWarning(s"完成加载配置文件")

  val redis_queuename = (confXml \ "env" \ "redis" \ "queuename").text

  private val redis_nodes = (confXml \ "env" \ "redis" \ "nodes" \ "node")
  private val nodeList: Seq[HostAndPort] = redis_nodes.map(node => {
    val host: String = node.attribute("host").mkString
    val port: Int = node.attribute("port").mkString.toInt
    new HostAndPort(host, port)
  })

  import scala.collection.JavaConverters._

  private val nodesSet: util.Set[HostAndPort] = nodeList.toSet.asJava
  val jedisCluster: JedisCluster = new JedisCluster(nodesSet)


  val neo4j_url = (confXml \ "env" \ "neo4j-pool" \ "url").text

  val neo4j_user = (confXml \ "env" \ "neo4j-pool" \ "user").text

  val neo4j_passwd = (confXml \ "env" \ "neo4j-pool" \ "passwd").text

  val neo4j_max_active = (confXml \ "env" \ "neo4j-pool" \ "maxQueryActive").text.toInt
  logWarning(s"最大查询连接数为${neo4j_max_active}")

  val page_sie: Int = (confXml \ "env" \ "neo4j-pool" \ "page_size").text.toInt

  val threshold: Int = (confXml \ "env" \ "neo4j-pool" \ "threshold").text.toInt

  val mysqlDs: DruidDataSource = new DruidDataSource

  val mysql_url = (confXml \ "env" \ "mysql" \ "url").text
  val mysql_user = (confXml \ "env" \ "mysql" \ "user").text
  val mysql_passwd = (confXml \ "env" \ "mysql" \ "passwd").text

  initMysql()
  private def initMysql(): Unit = {
    logWarning(s"mysql:${mysql_url},${mysql_user},${mysql_passwd}")
    mysqlDs.setDriverClassName("com.mysql.jdbc.Driver")
    mysqlDs.setUrl(mysql_url)
    mysqlDs.setUsername(mysql_user)
    mysqlDs.setPassword(mysql_passwd)
    mysqlDs.setInitialSize(5)
    mysqlDs.setMaxActive(10)
    //最多等待连接3秒钟
    mysqlDs.setMaxWait(3000)
    mysqlDs.setPoolPreparedStatements(true)
    mysqlDs.setMaxOpenPreparedStatements(10)
  }

  val batch_order_num: Int = (confXml \ "env" \ "batch_order_num").text.toInt

  val compute_batch_size: Int = (confXml \ "env" \ "compute_batch_size").text.toInt
  logWarning(s"计算计算批次上限:${compute_batch_size}")

  val performance_update_time: Int = (confXml \ "env" \ "performance_update_times").text.toInt * 1000 * 3600

  val performance_partintion_num: Int = (confXml \ "env" \ "performance_partintion_num").text.toInt
  logWarning(s"行为表分区大小:${performance_partintion_num}")

  val joined_partiontion_num: Int = (confXml \ "env" \ "joined_partiontion_num").text.toInt
  logWarning(s"join后的结果表的分区大小:${joined_partiontion_num}")
}
