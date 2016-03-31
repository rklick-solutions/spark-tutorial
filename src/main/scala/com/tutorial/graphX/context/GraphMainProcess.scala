package com.tutorial.graphX.context

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.SQLContext


/**
  * Created by ved on 31/3/16.
  */
object GraphMainProcess {
  val PRIMARY_INDEX = "primaryIndex"
  val VERTEX_ATTR = "vertexAttr"
  val EDGE_ATTR = "edgeAttr"

  def main(args: Array[String]) {

    val levelMap = Map("1" -> "Year")
    //val attrMap = Map(PRIMARY_INDEX -> "id", VERTEX_ATTR -> "name")
    // val sc = SparkCommon.sparkContext
    //val sqlContext = SparkCommons.sqlContext

    val sc = SparkCommon.sparkContext
    val sqlContext = new SQLContext(sc)



    val dataFrame = sqlContext.read.format("csv").option("header", "true")
      //.option("inferSchema", "true").load(path)
      .option("inferSchema", "true").load("/home/supriya/csv_data/U.SDisease.csv")
    //.option("inferSchema", "true").load("/home/supriya/csv_data/ExportData1.csv")
    val rGraphComponent = RGraphComponent(Some("ID"), None, "LocationAbbr", "", levelMap)
    //val workflow = Workflow(Nil, Nil, Map())
    //val kqMessage = KQMessage("app_id", "userid", "requestId", "campaignId", "csv", workflow)
    println(s":::::::::::::::::>>>>>>>>>>>>>>${System.currentTimeMillis()}")
    GraphContext.processGraph(dataFrame, rGraphComponent)
  }


}

case class RGraphComponent(primaryIndex: Option[String] = Some("primaryId"), relation: Option[String],
                           vertexAttr: String, edgeAttr: String, levels: Map[String, String])
