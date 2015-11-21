package com.tutorial.utils

/**
  * Created by ved on 21/11/15.
  */
object SparkCommon {

  val conf = {
    new SparkConf().setAppName("Spark Tutorial")
  }
  val sc = new SparkContext(conf)

}
