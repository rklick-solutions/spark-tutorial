package com.tutorial.sparksql.DataSources

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.SQLContext

/**
  * Created by ved on 29/1/16.
  */
object CsvFile {

  val sc = SparkCommon.sparkContext

  val sqlContext = SparkCommon.sparkSQLContext

  def main(args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/cars.csv")
    df.show()
    df.printSchema()

    val selectedData = df.select("year", "model")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
    selectedData.show()


    //.save(s"src/main/resources/${UUID.randomUUID()}")
    //println("OK")

  }

}


