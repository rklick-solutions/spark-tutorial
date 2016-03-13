package com.tutorial.graphX.Rklick_Implement

import com.tutorial.graphX.Rklick_Implement
import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, StronglyConnectedComponents}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal


/**
  * Created by ved on 12/3/16.
  */
trait GraphXMain {
  type GraphRDD = RDD[Map[String, Any]]
  val logger: Logger = LoggerFactory.getLogger(classOf[GraphXMain])


  /**
    * Create edges from dataframe
    *
    * @param df
    * @param graph
    * @return
    */
  def getEdges(df: DataFrame, graph: GraphConnection): RDD[Edge[String]] = {
    df.map {
      case row: Row =>
        val primaryValue = row.getAs[Any](graph.primaryIndex).toString.toLong
        val relIndex = row.getAs[Any](graph.relation).toString.toLong
        val edgeAttr = row.getAs[Any](graph.edgeAttr).toString
        Edge(primaryValue, relIndex, edgeAttr)
    }
  }

  /**
    * Create vertices from dataframe
    *
    * @param df    The DataFrame
    * @param graph The domain.GraphComponent
    * @return
    */
  def getVertices(df: DataFrame, graph: GraphConnection): RDD[(Long, String)] = df.map {
    case row =>
      val primaryValue = row.getAs[Any](graph.primaryIndex).toString.toLong
      val vertexAttr = row.getAs[Any](graph.vertexAttr).toString
      (primaryValue, vertexAttr)
  }

  /**
    * Process graph
    *
    * @param df             The DataFrame
    * @param graphComponent The domain.GraphComponent
    */
  def processGraph(df: DataFrame, graphComponent: GraphConnection): Either[String, GraphRDD] = {
    try {
      val edges = getEdges(df, graphComponent)
      val vertices = getVertices(df, graphComponent)
      val graph = Graph(vertices, edges).cache()
      for {
        result <- applyGraphAlgorithm(graph, graphComponent.primaryIndex).right
      } yield result
    } catch {
      case NonFatal(t: Throwable) =>
        Left(t.getMessage)
    }
  }

  /**
    * Apply graph algorithm & find the output
    *
    * @param graph
    * @param primaryIndex
    * @return
    */
  def applyGraphAlgorithm(graph: Graph[String, String], primaryIndex: String): Either[String, GraphRDD] = {
    try {
      //Page rank Algorithm
      val pageRankGraph = PageRank.run(graph, 1, 0.001)
      // Page Rank join with graph object
      val graphWithPageRank = graph.outerJoinVertices(pageRankGraph.vertices) {
        case (id, attr, Some(pr)) => (pr, attr)
        case (id, attr, None) => (0.0, attr)
      }

      //Triangle Count Algorithm
      val triangleComponent = graph.partitionBy(PartitionStrategy.RandomVertexCut)
        .triangleCount().vertices

      //Triangle Count join with page rank graph object
      val triByGraph = graphWithPageRank.outerJoinVertices(triangleComponent) {
        case (id, (rank, attr), Some(tri)) => (rank, tri, attr)
        case (id, (rank, attr), None) => (rank, 0, attr)
      }

      //Connected Component Algorithm
      val cComponent = ConnectedComponents.run(graph).vertices
      //Connected Component join with triangle component graph object
      val ccByGraph = triByGraph.outerJoinVertices(cComponent) {
        case (id, (rank, tri, attr), Some(cc)) => (rank, tri, cc, attr)
        case (id, (rank, tri, attr), None) => (rank, tri, -1L, attr)
      }

      //Strongly Connected Component Algorithm
      val stronglyConnected = StronglyConnectedComponents.run(graph, 3).vertices
      //Strongly Connected Component join with connected component object
      val stByGraph = ccByGraph.outerJoinVertices(stronglyConnected) {
        case (id, (rank, tri, cc, attr), Some(st)) => (rank, tri, cc, st, attr)
        case (id, (rank, tri, cc, attr), None) => (rank, tri, cc, id.toLong, attr)
      }

      //Result output
      val result = stByGraph.triplets.map {
        case line =>
          val relation = Map("from" -> line.srcId.toString, "to" -> line.dstId.toString, "type" -> line.attr)
          Map(primaryIndex -> line.srcId.toLong, "pageRank" -> line.srcAttr._1.toDouble,
            "triangleComponent" -> line.srcAttr._2.toInt, "connectedComponent" -> line.srcAttr._3.toLong,
            "stConnectedComponent" -> line.srcAttr._4.toLong, "link_relation" -> relation)
      }

      //Find top 10 users
      println("top 10 user")
      println(graphWithPageRank.vertices.top(10)(Ordering.by(_._2._1)).mkString("\n"))
      Right(result)
    } catch {
      case NonFatal(t: Throwable) => Left(t.getMessage)
    }
  }
}
