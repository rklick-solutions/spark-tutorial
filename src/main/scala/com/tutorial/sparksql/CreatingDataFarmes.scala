package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon

/**
  * Created by ved on 29/1/16.
  */
object CreatingDataFarmes {

  val sc = SparkCommon.sparkContext

  /**
    * Create a Scala Spark SQL Context.
    */
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {
    /**
      * Create the DataFrame
      */
    val df = sqlContext.read.json("src/main/resources/cars.json")

    /**
      * Show the Data
      */
    df.show()

  }
}
