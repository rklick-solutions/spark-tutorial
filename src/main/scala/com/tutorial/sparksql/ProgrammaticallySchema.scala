package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Method for creating DataFrame is through programmatic interface
  * Programmatically Specifying the Schema
  *
  * Created by ved on 19/1/16.
  */
object ProgrammaticallySchema {
  val sc = SparkCommon.sparkContext
  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  //sc is an existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    // Create an RDD
    val fruit = sc.textFile("src/main/resources/fruits.txt")

    // The schema is encoded in a string
    val schemaString = "id name"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    schema.foreach(println)

    // Convert records of the RDD (fruit) to Rows.
    val rowRDD = fruit.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    rowRDD.foreach(println)

    // Apply the schema to the RDD.
    val fruitDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    fruitDataFrame.foreach(println)

    // Register the DataFrames as a table.
    fruitDataFrame.registerTempTable("fruit")

    /**
      * SQL statements can be run by using the sql methods provided by sqlContext.
      */
    val results = sqlContext.sql("SELECT * FROM fruit")
    results.show()


  }


}
