package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon


/**
  * Created by ved on 29/1/16.
  */
object InferringTheSchema {

  val sc = SparkCommon.sparkContext

  val sqlContext = SparkCommon.sparkSQLContext

  import sqlContext.implicits._

  def main(args: Array[String]) {

    /**
      * Create RDD and Apply Transformations
      */

    val fruits = sc.textFile("src/main/resources/fruits.txt")
      .map(_.split(","))
      .map(frt => Fruits(frt(0).trim.toInt, frt(1), frt(2).trim.toInt))
      .toDF()

    /**
      * Store the DataFrame Data in a Table
      */
    fruits.registerTempTable("fruits")

    /**
      * Select Query on DataFrame
      */
    val records = sqlContext.sql("SELECT * FROM fruits")


    /**
      * To see the result data of allrecords DataFrame
      */
    records.show()

  }
}

case class Fruits(id: Int, name: String, quantity: Int)