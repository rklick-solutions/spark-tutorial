package com.tutorial.graphX.Rklick_Implement

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.SQLContext

/**
  * Created by ved on 12/3/16.
  */
object GraphXImplement {

  val sc = SparkCommon.sparkContext


  val sqlContext = new SQLContext(sc)

}