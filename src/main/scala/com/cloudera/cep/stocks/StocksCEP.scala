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

  case class Stock(symbol: String, price: Double)

  def main(args: Array[String]): Unit = {

    val ssc = SparkConfUtil.createSparkContext(20000, "StocksCEP")
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    val stockRDD1 = sc.parallelize(1 to 100).map(i => Stock(randomSymbol(), randomPrice()))
    val stockStream1 = new ConstantInputDStream[Stock](ssc, stockRDD1)
    registerDStreamAsTable(stockStream1, "stocks")

    sql("SELECT * FROM stocks").map(_.copy()).print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }

  //
  def randomSymbol() : String = {
    var idx = (Math.random() * 100).toInt

    var symbols = Array("GOOG", "FB", "AAPL")

    var length = symbols.length

    idx = idx % length

    symbols(idx)

  }

  def randomPrice() : Double = {
    var v = Math.random() * 100

    v
  }
}
