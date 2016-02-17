package com.tutorial.sparksql

import org.apache.spark.sql
import org.apache.spark.sql.SQLContext

import com.tutorial.utils.SparkCommon

/**
  * Created by ved on 16/2/16.
  */
object BasicQueryExample {

  val sc = SparkCommon.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    //import sqlContext.implicits._

    val input = sqlContext.read.json("src/main/resources/cars1.json")

    input.registerTempTable("Cars1")


    val result = sqlContext.sql("SELeCT * FROM Cars1")

    result.show()

    sqlContext.sql("SELECT COUNT(*) FROM Cars1").collect().foreach(println)

    sqlContext.sql("SELECT name, COUNT(*) AS cnt FROM Cars1 WHERE name <> '' GROUP BY name ORDER BY cnt DESC LIMIT 10")
      .collect().foreach(println)


  }

}

case class Cars1(name: String)


