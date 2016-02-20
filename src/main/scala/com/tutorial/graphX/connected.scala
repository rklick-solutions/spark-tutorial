package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 19/2/16.
  */
object connected {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * Load the edges from facebook_edges file
      */

    val facebookEdgesFile = sc.textFile("src/main/resources/facebook_edges.csv")

    /**
      * Convert the facebookEdgesFile RDD into the RDD of edges.
      */

    val edges = facebookEdgesFile.map(_.split(",")).map(e => Edge(e(0).
      toLong, e(1).toLong, e(2)))

    /**
      * Load the vertices from facebook_nodes file. 
      */

    val facebookVerticesFile = sc.textFile("src/main/resources/facebook_nodes.csv")

    /**
      * Map the vertices.
      */

    val vertices = facebookVerticesFile.map(_.split(",")).map(e =>
      (e(0).toLong, e(1)))

    /**
      * Create the graph object.
      */

    val graph = Graph(vertices, edges)


    /**
      * Calculate the connected components.
      */
    val cc = graph.connectedComponents

    /**
      * Find the vertices for the connected components (which is a subgraph).
      */

    val ccVertices = cc.vertices

    val ccEdges = cc.edges

    /**
      * Print the ccVertices.
      */

    ccVertices.collect.foreach(println)

    /**
      * Print the ccEdges.
      */

    ccEdges.collect.foreach(println)

  }

}
