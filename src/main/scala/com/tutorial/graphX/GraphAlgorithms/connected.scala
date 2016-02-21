package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * connected:
  * an undirected graph is a subgraph in which any two vertices are connected to each other by paths,
  * and which is connected to no additional vertices in the supergraph.
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

    val fbEdges = facebookEdgesFile.map(_.split(",")).map(e => Edge(e(0).
      toLong, e(1).toLong, e(2)))

    /**
      * Load the vertices from facebook_nodes file. 
      */

    val facebookVerticesFile = sc.textFile("src/main/resources/facebook_nodes.csv")

    /**
      * Map the vertices.
      */

    val fbVertices = facebookVerticesFile.map(_.split(",")).map(e =>
      (e(0).toLong, e(1)))

    /**
      * Create the graph object.
      */

    val graph = Graph(fbVertices, fbEdges)


    /**
      * Calculate the connected components.
      */
    val connectedFb = graph.connectedComponents

    /**
      * Find the vertices for the connected components (which is a subgraph).
      */

    val connectedFbVertices = connectedFb.vertices

    val connectedFbEdges = connectedFb.edges

    /**
      * Print the ccVertices.
      */

    connectedFbVertices.collect.foreach(println)

    /**
      * Print the ccEdges.
      */

    connectedFbEdges.collect.foreach(println)

  }

}
