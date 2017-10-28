package com.lakala.finance.anti_fraud

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.lakala.finance.anti_fraud.biz.{Graph2Var, QueryGraphTask}
import com.lakala.finance.anti_fraud.conf.Graph2VarConfig
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import redis.clients.jedis.{JedisCluster, JedisPubSub}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/8/8.
  */
object Graph2Var extends Logging {
  private val mesgQueue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String]()

  def main(args: Array[String]) {
    val config_path: String = args(0)
    val confXmlPath: String = s"${config_path}/Graph2VarConfig.xml"
    val config = new Graph2VarConfig(confXmlPath)
    val sparkConf = new SparkConf().setAppName("Graph2Var")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)


    val orderQ = new LinkedBlockingQueue[String]()
    val oneDegreeQ = new LinkedBlockingQueue[ArrayBuffer[String]]()
    val twoDegreeQ = new LinkedBlockingQueue[ArrayBuffer[String]]()
    val readWriteLock: ReentrantReadWriteLock = new ReentrantReadWriteLock()

    val read_thread_num: Int = config.neo4j_max_active
    val pool: ExecutorService = Executors.newFixedThreadPool(read_thread_num + 1)
    for (i <- 0 until read_thread_num) {
      pool.submit(new QueryGraphTask(config, mesgQueue, readWriteLock, orderQ, oneDegreeQ, twoDegreeQ))
    }
    pool.submit(new Graph2Var(config, hc, readWriteLock, orderQ, oneDegreeQ, twoDegreeQ))

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
