package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon

/**
  * Created by ved on 20/1/16.
  */
object CreatingDatasets {

  val sc = SparkCommon.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    import sqlContext.implicits._

    val lines = sqlContext.read.text("src/main/resources/test_file.txt").as[String]
    val words = lines
      .flatMap(_.split(" "))
      .filter(_ != "")
      .groupBy(_.toLowerCase)
      .count()
      .show()

    /**
      * Encoders are also created for case classes.
      */
      //case class Cars(name: String, kph: Long)
    val ds = Seq(Cars("lamborghini", 32)).toDS()
    ds.show()

    /**
      *DataFrames can be converted to a Dataset by providing a class.
      *  Mapping will be done by name.
      */

    val people = sqlContext.read.json("src/main/resources/cars.json").as[Cars]
    
    people.show()



  }
}

case class Cars(name: String, kph: Long)
