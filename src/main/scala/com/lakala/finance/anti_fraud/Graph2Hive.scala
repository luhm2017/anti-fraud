package com.lakala.finance.anti_fraud

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

import com.lakala.finance.anti_fraud.biz.{QueryNeo4jTask, WriteGraphData2Hive}
import com.lakala.finance.anti_fraud.conf.Neo2HiveConfig
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import redis.clients.jedis.{JedisCluster, JedisPubSub}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/8/7.
  */
object Graph2Hive extends Logging {
  private val mesgQueue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String]()

  def main(args: Array[String]) {
    val config_path: String = args(0)
    val confXmlPath: String = s"${config_path}/Neo4j2HiveConf.xml"
    logInfo(s"程序相关配置为:${confXmlPath}")
    val config = new Neo2HiveConfig(confXmlPath.trim)
    val sparkConf = new SparkConf().setAppName("Graph2Hive")
    //    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val thread_num: Int = config.neo4j_max_active + 1
    val pool: ExecutorService = Executors.newFixedThreadPool(thread_num)
    logWarning(s"成功新建连接数${thread_num}的线程池处理任务")
    val degree1Queue = new LinkedBlockingQueue[ArrayBuffer[String]]()
    val degree2Queue = new LinkedBlockingQueue[ArrayBuffer[String]]()
    val ordernoQueue = new LinkedBlockingQueue[(String, String, Int, Int, Int)]()
    val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock()

    for (i <- 0 until config.neo4j_max_active) {
      pool.submit(new QueryNeo4jTask(config, mesgQueue, lock, degree1Queue, degree2Queue, ordernoQueue))
    }
    pool.submit(new WriteGraphData2Hive(config, lock, sqlContext, degree1Queue, degree2Queue, ordernoQueue))
    logWarning(s"提交写入任务")

    val jedis: JedisCluster = config.jedisCluster
    jedis.subscribe(new GraphListner(), config.redis_queuename)
  }

  private class GraphListner extends JedisPubSub {
    override def onSubscribe(ch: String, subscribedChannels: Int): Unit = {
      logWarning(s"成功建立redis监听:channel:${ch},subscribed:${subscribedChannels}")
    }

    override def onMessage(channel: String, message: String): Unit = {
      //      Thread.sleep(3000)
      mesgQueue.put(message)
      logWarning(s"成功收到redis消息,mesg:${message},当前待处理的消息量为:${mesgQueue.size()}")
    }
  }

}
