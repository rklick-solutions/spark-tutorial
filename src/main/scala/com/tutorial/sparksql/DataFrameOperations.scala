package com.tutorial.sparksql


import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame


/**
  * Created by ved on 19/1/16.
  */
object DataFrameOperations {

  val sc = SparkCommon.sparkContext

  /**
    * Use the following command to create SQLContext.
    */
  val ssc = SparkCommon.sparkSQLContext

  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  def main(args: Array[String]) {

    /**
      * Create the DataFrame
      */
    val employee = "src/main/resources/employee.json"

    /**
      * read the JSON document
      * Use the following command to read the JSON document named employee.json.
      * The data is shown as a table with the fields − id, name, age and employeeCode.
      */
    val empDataFrame: DataFrame = ssc.read.format("json").options(schemaOptions).load(employee)

    /**
      * Show the Data
      * If you want to see the data in the DataFrame, then use the following command.
      */
    empDataFrame.show()

    /**
      * printSchema Method
      * If you want to see the Structure (Schema) of the DataFrame, then use the following command
      */
    empDataFrame.printSchema()

    /**
      * Select Method
      * Use the following command to fetch name-column among three columns from the DataFrame
      */
    empDataFrame.select("name").show()

    /**
      * Filter used to
      * employees whose age is greater than 23 (age > 23).
      */
    empDataFrame.filter(empDataFrame("age") > 23).show()

    /**
      * groupBy Method
      * counting the number of employees who are of the same age.
      */
    empDataFrame.groupBy("age").count().show()


  }


}

