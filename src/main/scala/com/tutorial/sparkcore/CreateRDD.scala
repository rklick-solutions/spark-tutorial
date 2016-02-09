package com.tutorial.sparkcore

import com.tutorial.utils.SparkCommon

/**
    * An example to create RDD from different data sources
    * Created by ved on 7/1/16.
    */
object CreateRDD {

  /**
    * Create a Scala Spark Context.
    */
  val sc = SparkCommon.sparkContext
  def main(args: Array[String]) {

    /**
      * Create RDDs using parallelize() method of SparkContext
      */
    val lines = sc.parallelize(List("pandas", "i like pandas"))
    lines.collect().map(println)

    /**
      * Create RDDs is to load data from external storage
      */
    val rddDataset = sc.textFile("src/main/resources/test_file.txt")
    rddDataset.collect().map(println)
    //sc.stop()
  }

}
