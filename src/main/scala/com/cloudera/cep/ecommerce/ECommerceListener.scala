package com.cloudera.cep.ecommerce

import org.apache.spark.sql.Row

/**
 * Created by jayant on 7/23/15.
 */
object ECommerceListener {

  def listen(row : Row): Unit = {
    var str = row.toString()
    print(str)

    str = ""
  }
}
