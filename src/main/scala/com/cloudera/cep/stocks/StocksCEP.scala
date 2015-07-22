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

    // create StreamSQLContext
    val ssc = SparkConfUtil.createSparkContext(20000, "StocksCEP")
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    // create DStream of stocks
    val stockRDD1 = sc.parallelize(1 to 100).map(i => Stock(randomSymbol(), randomPrice()))
    val stockStream1 = new ConstantInputDStream[Stock](ssc, stockRDD1)
    registerDStreamAsTable(stockStream1, "stocks")

    // run queries
    runQueries(streamSqlContext)


    // start/stop
    ssc.start()
    ssc.awaitTerminationOrTimeout(300 * 1000)
    ssc.stop()
  }

  // run the queries
  def runQueries(streamSqlContext : StreamSQLContext): Unit = {

    // display all the stock ticks
    //streamSqlContext.sql("SELECT * FROM stocks").map(_.copy()).print()

    // display count/avg of ticks for each symbol
    //streamSqlContext.sql("SELECT symbol, count(*), avg(price) FROM stocks group by symbol").map(_.copy()).print()

    // display symbols for which there is rapid drop-off : change > 96
    streamSqlContext.sql("SELECT symbol, avg(price), (max(price) - min(price)) as chg FROM stocks group by symbol having chg > 96.0").map(_.copy()).print()
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
}
