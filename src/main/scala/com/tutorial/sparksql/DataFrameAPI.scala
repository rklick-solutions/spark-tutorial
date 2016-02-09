package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame

/**
  * Created by ved on 9/2/16.
  * DataFarme API Example Using Different types of Functionality.
  * Diiferent type of DataFrame operatios are :
  */

  /**
    * Action:
    * Action are operations (such as take, count, first, and so on) that return a value after
    * running a computation on an DataFrame.
    */
object DataFrameAPI {

  val sc = SparkCommon.sparkContext

  /**
    * Use the following command to create SQLContext.
    */
  val ssc = SparkCommon.sparkSQLContext

  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  def main(args: Array[String]) {

    val employee = "src/main/resources/employee.json"

    val empDataFrame: DataFrame = ssc.read.format("json").options(schemaOptions).load(employee)

    /**
      * Some Action Operation with examples:
      * show()
      * If you want to see top 20 rows of DataFrame in a tabular form then use the following command.
      */

    empDataFrame.show()


    /**
      * show(n)
      * If you want to see n rows of DataFrame in a tabular form then use the following command.
      */

    empDataFrame.show(2)


    /**
      * take()
      * take(n) Returns the first n rows in the DataFrame.
      */
    empDataFrame.take(2).foreach(println)


    /**
      * count()
      * Returns the number of rows.
      */

    empDataFrame.groupBy("age").count().show()


    /**
      * head()
      * head () is used to returns first row.
      */

    val resultHead = empDataFrame.head()

    println(resultHead.mkString(","))

    /**
      * head(n)
      * head(n) returns first first n rows.
      */

    val resultHeadNo = empDataFrame.head(3)

    println(resultHeadNo.mkString(","))

    /**
      * first()
      * Returns the first row.
      */

    val resultFirst = empDataFrame.first()

    println("fist:" + resultFirst.mkString(","))


    /**
      * collect()
      * Returns an array that contains all of Rows in this DataFrame.
      */

    val resultCollect = empDataFrame.collect()

    println(resultCollect.mkString(","))

  }


}
