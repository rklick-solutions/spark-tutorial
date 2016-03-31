package com.tutorial.graphX.context

import java.util.Date

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec
import org.apache.spark.sql.functions._


/**
  * Created by ved on 31/3/16.
  */
object GraphContext extends GraphHelper {


  /**
    *
    * @param graphComponent
    * @param levelMaps
    * @param df
    * @return
    */
  private def getVertices(levelMaps: Map[String, String], df: DataFrame,
                          graphComponent: RGraphComponent): RDD[(VertexId, (String, Long, Map[String, String]))] = {
    println(s"START TIME:::::::${System.currentTimeMillis()}")
    df.map {
      case row =>
        val pIndex = graphComponent.primaryIndex.getOrElse("primaryId")
        val pIndexValue = !pIndex.isEmpty match {
          case true => row.getAs[Any](pIndex).toString.toLong
          case false => -1
        }
        val vertexAttr = graphComponent.vertexAttr
        val vertexValue = !vertexAttr.isEmpty match {
          case true => row.getAs[Any](vertexAttr).toString
          case false => ""
        }

        val publishDate = /*row.getAs[Timestamp](PUBLISH_DATE).getTime*/ new Date().getTime
        val vertexAttrMap = (levelMaps.map {
          case (key, value) => (key, row.getAs[Any](value).toString)
        })
        (pIndexValue, (vertexValue, publishDate, vertexAttrMap))
    }
  }

  /**
    *
    * @param graphComponent
    * @param df
    * @return
    */
  private def getEdges(graphComponent: RGraphComponent, df: DataFrame): RDD[Edge[String]] = {
    df.map {
      case row =>
        val pIndex = graphComponent.primaryIndex.getOrElse("primaryId")
        val pIndexValue = !pIndex.isEmpty match {
          case true => row.getAs[Any](pIndex).toString.toLong
          case false => -1
        }
        val edgeAttr = graphComponent.edgeAttr
        val edgeValue = !edgeAttr.isEmpty match {
          case true => row.getAs[Any](edgeAttr).toString
          case false => ""
        }
        Edge(pIndexValue, pIndexValue, edgeValue) //TODO: dstId
    }
  }

  /**
    *
    * @param dataFrame
    * @param graphComponent
    */
  def processGraph(dataFrame: DataFrame, graphComponent: RGraphComponent): Either[Exception, Boolean] = {

    val levelMap = graphComponent.levels
    val list = List(graphComponent.primaryIndex.getOrElse("primaryId"), graphComponent.vertexAttr) ++
      graphComponent.levels.values.toList
    val modDf = dataFrame.select(list.distinct.map { colName => dataFrame.col(colName) }: _*)
    //val newDF = filteredDf(modDf, modDf.sqlContext.emptyDataFrame, levelMap.values.toList)
    val category = levelMap.get("1").head
    val categories = modDf.groupBy(col(category)).count().map(row => row.getAs[Any](category).toString).distinct().collect().toList
    println(s"CATEGORIES>>>>>>>>>>>>>>${categories}")
    val newDF = updateDF(categories, modDf, modDf.sqlContext.emptyDataFrame, category)
    newDF.show()
    println(s">>>>>>>>>>>>>>>>>>END TIME:::::::::::${System.currentTimeMillis()}")
    val vertices = getVertices(levelMap, newDF, graphComponent)
    val edges = getEdges(graphComponent, newDF)
    val graph = Graph(vertices, edges)
    //toBasicGraph(newDF, graph, levelMap, msg)
    Right(true)
  }

  /*/**
    *
    * @param verticesDf
    * @param graphs
    * @param levelMap
    */
  def toBasicGraph(verticesDf: DataFrame, graphs: Graph[(String, Long, Map[String, String]), String],
                   levelMap: Map[String, String], msg: KQMessage): Either[RException, Boolean] = {

    val table = s"${msg.requestId}_graphx"
    verticesDf.registerTempTable(table)

    val nodes = graphs.triplets.map {
      case graph => Nodes(graph.srcId.toString(), Style(graph.srcAttr._1), graph.srcAttr._2)
    }
    //Processing Node
    val resultNode = mergeNode(nodes, levelMap, verticesDf, table) //TODO:
    //Processing Links
    val links: RDD[Links] = verticesDf.sqlContext.sparkContext.emptyRDD
    val resultLinks = mergeLink(links, levelMap, graphs)
    error(s"PROCESSING COMPLETED TIME::::::::::::::::${System.currentTimeMillis}")
    /*for {
      status <- ESUtility.saveGraphDataToEs(resultNode, resultLinks, msg).right
    } yield status*/
    Right(true)
  }*/

  /*def filteredDf(df: DataFrame, resultDf: DataFrame, categories: List[String]): DataFrame = {
    @tailrec
    def inner(categoryList: List[String], result: DataFrame): DataFrame = {
      if (categoryList.isEmpty) {
        result
      } else {
        val category = categoryList.head
        val categoryDatas = df.groupBy(col(category)).count().map(row => row.getAs[Any](category).toString)
          .distinct().collect().toList
        val newDF = updateDF(categoryDatas, df, resultDf, category)
        inner(categoryList.tail, newDF)
      }
    }
    inner(categories, resultDf)
  }*/

  /**
    *
    * @param column
    * @param inputDF
    * @param resultDF
    * @return
    */
  def updateDF(column: List[String], inputDF: DataFrame, resultDF: DataFrame, category: String): DataFrame = {
    @tailrec
    def inner(columns: List[String], result: DataFrame): DataFrame = {
      if (columns.isEmpty) {
        result
      } else {
        val columnName = columns.head
        val startTime = System.currentTimeMillis()
        println(s"UPDATE DF FOR ${columnName} START TIME>>>>>>>>>>..${startTime}")
        val df = inputDF.filter(col(category) === columnName)
        val table = s"test_${columnName.replaceAll("-", "")}"
        df.registerTempTable(table)
        val filterData = df.sqlContext.sql(s"select * from ${table} limit 5")
        df.sqlContext.dropTempTable(table)
        val resultDf = if (!result.rdd.isEmpty()) {
          result.unionAll(filterData)
        } else {
          filterData
        }
        println(s"UPDATE DF FOR ${columnName} END TIME>>>>>>>>>>.....${System.currentTimeMillis() - startTime}")
        inner(columns.tail, resultDf)
      }
    }
    inner(column, resultDF)
  }

}

