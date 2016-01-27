package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by ved on 19/1/16.
  * Method for creating DataFrame is through programmatic interface
  * Programmatically Specifying the Schema
  */
object ProgrammaticallySchema {
  val sc = SparkCommon.sparkContext
  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  //sc is an existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    // Create an RDD
    val employee = sc.textFile("src/main/resources/employee.txt")

    // The schema is encoded in a string
    val schemaString = "name age"


    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (employee) to Rows.
    val rowRDD = employee.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val employeeDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    employeeDataFrame.registerTempTable("employee")

    /**
      * SQL statements can be run by using the sql methods provided by sqlContext.
      */
    val results = sqlContext.sql("SELECT name FROM employee")

    /**
      * The results of SQL queries are DataFrames and support all the normal RDD operations.
      * The columns of a row in the result can be accessed by field index or by field name.
      *
      */
    results.map(t => "Name: " + t(0)).collect().foreach(println)


  }


}
