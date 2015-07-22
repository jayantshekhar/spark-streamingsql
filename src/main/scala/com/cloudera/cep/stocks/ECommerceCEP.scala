package com.cloudera.cep.stocks

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.examples.SparkConfUtil
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
 * Created by jayant on 7/22/15.
 */
object ECommerceCEP {

  case class Transaction(customerId : Long, city: String, amount: Double)

  case class Click(customerId : Long, city: String)

  case class Location(customerId : Long, city: String)

  def main(args: Array[String]): Unit = {

    // create StreamSQLContext
    val ssc = SparkConfUtil.createSparkContext(20000, "ECommerceCEP")
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    // create DStream of transactions
    val transactionRDD1 = sc.parallelize(1 to 100).map(i => Transaction(randomCustomerId(), randomCity(), randomAmount()))
    val transactionStream1 = new ConstantInputDStream[Transaction](ssc, transactionRDD1)
    registerDStreamAsTable(transactionStream1, "transactions")

    // create DStream of clicks
    val clickRDD1 = sc.parallelize(1 to 100).map(i => Click(randomCustomerId(), randomCity()))
    val clickStream1 = new ConstantInputDStream[Click](ssc, clickRDD1)
    registerDStreamAsTable(clickStream1, "clicks")

    // create DStream of customer locations
    val locationRDD1 = sc.parallelize(1 to 100).map(i => Location(randomCustomerId(), randomCity()))
    val locationStream1 = new ConstantInputDStream[Location](ssc, locationRDD1)
    registerDStreamAsTable(locationStream1, "locations")

    // run queries
    runQueries(streamSqlContext)

    // start/stop
    ssc.start()
    ssc.awaitTerminationOrTimeout(300 * 1000)
    ssc.stop()
  }

  // run the queries
  def runQueries(streamSqlContext : StreamSQLContext): Unit = {

    streamSqlContext.sql("SELECT * FROM transactions").map(_.copy()).print()

    streamSqlContext.sql("SELECT * FROM clicks").map(_.copy()).print()

    // display all the unique cities
    streamSqlContext.sql("SELECT distinct city FROM locations").map(_.copy()).print()

  }

  // generate random city
  def randomCity() : String = {
    var idx = (Math.random() * 100).toInt

    var symbols = Array("San Francisco", "Los Angeles", "New York")

    var length = symbols.length

    idx = idx % length

    symbols(idx)

  }

  // generate random price
  def randomAmount() : Double = {
    var v = Math.random() * 100

    v
  }

  // generate random price
  def randomCustomerId() : Int = {
    var v = (Math.random() * 100).toInt

    v
  }
}
