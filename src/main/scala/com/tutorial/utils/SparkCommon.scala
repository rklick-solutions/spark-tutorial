package com.tutorial.utils

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ved on 21/11/15.
  */
object SparkCommon {

  val conf = {
    new SparkConf(false).setMaster("local[*]").setAppName("Spark Tutorial")
  }
  val sc = new SparkContext(conf)

}
