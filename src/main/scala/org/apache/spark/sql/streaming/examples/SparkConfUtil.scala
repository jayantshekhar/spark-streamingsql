package org.apache.spark.sql.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Duration}

/**
 * Created by jayant on 7/21/15.
 */
object SparkConfUtil {

  def setConf(conf: SparkConf): Unit = {
    conf.setMaster("local[10]")
    conf.set("spark.broadcast.compress", "false")
    conf.set("spark.shuffle.compress", "false")
  }

  def createSparkContext(duration : Long, appName : String): StreamingContext = {

    val batchInterval: Duration = new Duration(duration)
    val sparkConf: SparkConf = new SparkConf().setAppName(appName)
    setConf(sparkConf)
    val ssc = new StreamingContext(sparkConf, batchInterval)

    return ssc
  }

}

