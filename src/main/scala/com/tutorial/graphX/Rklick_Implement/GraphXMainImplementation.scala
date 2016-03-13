package com.tutorial.graphX.Rklick_Implement

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.SQLContext

/**
  * Created by ved on 12/3/16.
  */
object GraphXMainImplementation extends GraphXMain {

  val sc = SparkCommon.sparkContext

  val sqlContext = new SQLContext(sc)


  /**
    *
    * @return
    */
  def main(args: Array[String]) {
    val SCHEMA_OPTIONS = Map("header" -> "true", "inferSchema" -> "true")
    val path = "src/main/resources/graph_data.csv"
    val df = sqlContext.read.format("csv").options(SCHEMA_OPTIONS).load(path)
    val graphComponent = GraphConnection("ID", "User", "Relationship", "RelationId")
    processGraph(df, graphComponent) match {
      case Right(data) => println(s"GraphX Implementation Done.")
        sqlContext.sparkContext.stop()
      case Left(error) => println(s"See Error In graphX:::${error}")
        sqlContext.sparkContext.stop()
    }
  }
}