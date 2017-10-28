package com.lakala.finance.anti_fraud.conf


import java.io.InputStream
import java.util

import com.alibaba.druid.pool.DruidDataSource
import org.apache.spark.Logging
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.immutable.Seq
import scala.xml.{Elem, XML}

/**
  * Created by longxiaolei on 2017/8/7.
  */
class Neo2HiveConfig(path: String) extends Logging {
  private var confXml: Elem = null
  if ("jar".equals(path)) {
    val is: InputStream = getClass.getClassLoader.getResourceAsStream("Neo4j2HiveConf.xml")
    confXml = XML.load(is)
  } else {
    confXml = XML.load(path)
  }

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

  val degree1TableName = (confXml \ "env" \ "degree1HTName").text
  val degree2TableName = (confXml \ "env" \ "degree2HTName").text

  val mysqlDs: DruidDataSource = new DruidDataSource


  private val mysql_url = (confXml \ "env" \ "mysql" \ "url").text
  private val mysql_user = (confXml \ "env" \ "mysql" \ "user").text
  private val mysql_passwd = (confXml \ "env" \ "mysql" \ "passwd").text

  initMysql()

  def initMysql(): Unit = {
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

  val write_batch_time = (confXml \ "env" \ "writeBatchTime").text.toLong
  logWarning(s"写入hive批次的等待时间")
}