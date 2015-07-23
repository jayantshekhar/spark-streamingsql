package com.cloudera.cep.ecommerce

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.examples.SparkConfUtil
import org.apache.spark.sql.streaming.examples.StreamToStreamJoin.User
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
 * Created by jayant on 7/22/15.
 */
object ECommerceCEP {

  case class Transaction(customerId : Long, city: String, amount: Double, ip: String)

  case class Click(customerId : Long, city: String, productId : Long, ip: String)

  case class Location(customerId : Long, city: String, ip: String)


  def main(args: Array[String]): Unit = {

    // create StreamSQLContext
    val ssc = SparkConfUtil.createSparkContext(20000, "ECommerceCEP")
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    // create DStream of transactions
    val transactionRDD1 = sc.parallelize(1 to 100).map(i => Transaction(randomCustomerId(), randomCity(), randomAmount(), randomIP()))
    val transactionStream1 = new ConstantInputDStream[Transaction](ssc, transactionRDD1)
    registerDStreamAsTable(transactionStream1, "transactions")

    // create DStream of clicks
    val clickRDD1 = sc.parallelize(1 to 100).map(i => Click(randomCustomerId(), randomCity(), randomProductId(), randomIP()))
    val clickStream1 = new ConstantInputDStream[Click](ssc, clickRDD1)
    registerDStreamAsTable(clickStream1, "clicks")

    // create DStream of customer locations
    val locationRDD1 = sc.parallelize(1 to 100).map(i => Location(randomCustomerId(), randomCity(), randomIP()))
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

    // ---
    val dstream = streamSqlContext.sql("SELECT * FROM transactions")
    dstream.map(row => ECommerceListener.listen(row))

    // ---

    streamSqlContext.sql("SELECT * FROM transactions").map(_.copy()).print()

    streamSqlContext.sql("SELECT * FROM clicks").map(_.copy()).print()

    // display all the unique cities
    streamSqlContext.sql("SELECT distinct city FROM locations").map(_.copy()).print()

    // find customers with the highest total transaction amounts
    streamSqlContext.sql("SELECT customerId, sum(amount) as total FROM transactions group by customerId order by total desc limit 3").map(_.copy()).print()

    // find customers with the highest number of transactions
    streamSqlContext.sql("SELECT customerId, count(*) as num FROM transactions group by customerId order by num desc limit 3").map(_.copy()).print()

    // find customers who had records from 2 different cities in the duration
    streamSqlContext.sql(
      "SELECT transactions.customerId, count(*) as c FROM transactions JOIN clicks on transactions.customerId = clicks.customerId group by transactions.customerId, transactions.city having c > 1").map(_.copy()).print()
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

  // generate random customer id
  def randomCustomerId() : Int = {
    var v = (Math.random() * 100).toInt

    v
  }

  // generate random product id
  def randomProductId() : Int = {
    var v = (Math.random() * 100).toInt

    v
  }


  // generate random ip
  def randomIP() : String = {
    var v = (Math.random() * 100).toString

    v

  }

}

