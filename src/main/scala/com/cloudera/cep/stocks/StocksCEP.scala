package com.cloudera.cep.stocks

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.examples.SparkConfUtil
import org.apache.spark.sql.streaming.examples.StreamToStreamJoin.User
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
 * Created by jayant on 7/21/15.
 */
object StocksCEP {

  case class User(id: Int, name: String)

  def main(args: Array[String]): Unit = {
    // val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    // val sc = ssc.sparkContext

    val ssc = SparkConfUtil.createSparkContext(20000, "StocksCEP")
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    val userRDD1 = sc.parallelize(1 to 100).map(i => User(i / 2, s"$i"))
    val userStream1 = new ConstantInputDStream[User](ssc, userRDD1)
    registerDStreamAsTable(userStream1, "user1")

    val userRDD2 = sc.parallelize(1 to 100).map(i => User(i / 5, s"$i"))
    val userStream2 = new ConstantInputDStream[User](ssc, userRDD2)
    registerDStreamAsTable(userStream2, "user2")

    sql("SELECT * FROM user1 JOIN user2 ON user1.id = user2.id").map(_.copy()).print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }
}
