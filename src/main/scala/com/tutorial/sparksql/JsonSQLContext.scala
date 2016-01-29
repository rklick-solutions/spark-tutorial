package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame

/**
  * Created by anand on 1/9/16.
  */
object JsonSQLContext {
  val ssc = SparkCommon.sparkSQLContext

  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  def main(args: Array[String]) {

    val pocOnePath = "src/main/resources/sales.json"
    val dataset: DataFrame = ssc.read.format("json").options(schemaOptions).load(pocOnePath)

    dataset.printSchema()
    val columns = dataset.dtypes.filter { case (col, cType) =>
      println(cType)
      cType.startsWith("StringType")
    }.toMap.keySet.toList
    dataset.describe(columns: _*).show()


  }
}