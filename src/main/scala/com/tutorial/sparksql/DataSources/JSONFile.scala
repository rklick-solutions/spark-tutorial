package com.tutorial.sparksql.DataSources

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame

/**
  * Created by ved on 22/2/16.
  */
object JSONFile {

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
    val cars = "src/main/resources/cars.json"

    /**
      * read the JSON document
      * Use the following command to read the JSON document named cars.json.
      * The data is shown as a table with the fields − itemNo, name, speed and weight.
      */
    val empDataFrame: DataFrame = ssc.read.format("json").options(schemaOptions).load(cars)

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
      * cars whose speed is greater than 300 (speed > 300).
      */
    empDataFrame.filter(empDataFrame("speed") > 300).show()

    /**
      * groupBy Method
      * counting the number of cars who are of the same speed.
      */
    empDataFrame.groupBy("speed").count().show()


  }


}
