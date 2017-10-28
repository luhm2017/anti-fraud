./spark-shell -- master spark://datacenter17:7077,datacenter18:7077 --conf spark.locality.wait=1 --conf spark.driver.memory=1g  --conf spark.executor.cores=4 --total-executor-cores 12 --num-executors 3 --executor-memory 4g

package lakala.models

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

 
  val hc = new HiveContext(sc)


 val data = hc.sql(s"select * from lkl_card_score.phone_variable_yfq_creditcardrepayments_20170724_result").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除处理label、contact字段
        for(i <- 1 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
          else arr += 0.0
        }
        //label、contact数据单独处理
        (row.getString(0),Vectors.dense(arr.toArray))
    }
    //全量打分
    val modelGBDT = GradientBoostedTreesModel.load(sc,"hdfs://ns1/user/luhuamin/20170718/yfq/model")
    //打分
    val preditDataGBDT = data.map { point =>
      val prediction = modelGBDT.predict(point._2)
      (point._1, prediction)
    }
    //preditData保存
    preditDataGBDT.saveAsTextFile(s"hdfs://ns1/user/luhuamin/20170724/yfq/predictionAndLabels")
