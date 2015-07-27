package com.cloudera.cep.stocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.examples.SparkConfUtil
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
 * Created by jayant on 7/21/15.
 */
object StocksCEP {

  case class Stock(symbol: String, price: Double)

  case class StockDetail(symbol : String, d : String)

  def main(args: Array[String]): Unit = {

    // create StreamSQLContext
    val ssc = SparkConfUtil.createSparkContext(20000, "StocksCEP")
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    // create DStream of stocks and register it as a table
    val stockRDD1 = sc.parallelize(1 to 100).map(i => Stock(randomSymbol(), randomPrice()))
    val stockStream1 = new ConstantInputDStream[Stock](ssc, stockRDD1)
    registerDStreamAsTable(stockStream1, "stocks")

    // create dataframe of stock details and register it as a table
    val rdd : RDD[StockDetail] = sc.parallelize(stockDetails())
    val df = streamSqlContext.sqlContext.createDataFrame(rdd)
    df.registerTempTable("stockdetails")

    // run queries on the tables created
    runQueries(streamSqlContext)

    // start/stop
    ssc.start()
    ssc.awaitTerminationOrTimeout(300 * 1000)
    ssc.stop()
  }

  // run the queries
  def runQueries(streamSqlContext : StreamSQLContext): Unit = {

    // display all the stock ticks
    streamSqlContext.sql("SELECT * FROM stocks").map(_.copy()).print()

    // compute count/avg of ticks for each symbol
    streamSqlContext.sql("SELECT symbol, count(*), avg(price) FROM stocks group by symbol").map(_.copy()).print()

    // display symbols for which there is rapid drop-off in the duration : change > 96
    streamSqlContext.sql("SELECT symbol, avg(price), (max(price) - min(price)) as chg FROM stocks group by symbol having chg > 6.0").map(_.copy()).print()

    // compute the avg stock price over a window of 60 seconds sliding every 20 seconds
    streamSqlContext.sql("SELECT symbol, avg(price) FROM stocks OVER (WINDOW '60' SECONDS, SLIDE '20' SECONDS) group by symbol ").map(_.copy()).print()

    val joinedds = streamSqlContext.sql(
      "SELECT * FROM stocks s, stockdetails sd where s.symbol = sd.symbol")

    joinedds.map(_.copy()).print()

  }

  // generate random symbol
  def randomSymbol() : String = {
    var idx = (Math.random() * 100).toInt

    var symbols = Array("GOOG", "FB", "AAPL")

    var length = symbols.length

    idx = idx % length

    symbols(idx)

  }

  // generate random price
  def randomPrice() : Double = {
    var v = Math.random() * 100

    v
  }


  def stockDetails() : Array[StockDetail] = {
    var arr : Array[StockDetail] = new Array[StockDetail](3)

    arr(0) = StockDetail("GOOG", "Google")
    arr(1) = StockDetail("FB", "Facebook")
    arr(2) = StockDetail("AAPL", "Apple")

    arr
  }

}
