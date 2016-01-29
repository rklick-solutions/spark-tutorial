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

    // Create RDD and Apply Transformations
    val empl = sc.textFile("src/main/resources/employee.txt")
      .map(_.split(","))
      .map(emp => Employee(emp(0).trim.toInt, emp(1), emp(2).trim.toInt))
      .toDF()

    /**
      * Store the DataFrame Data in a Table
      */
    empl.registerTempTable("employee")

    /**
      * Select Query on DataFrame
      */
    val records = sqlContext.sql("SELECT * FROM employee")

    /**
      * To see the result data of allrecords DataFrame
      */
    records.show()

  }
}

case class Employee(id: Int, name: String, age: Int)