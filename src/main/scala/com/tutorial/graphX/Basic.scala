package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 18/2/16.
  */
object Basic {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {


    /**
    *First we load the vertex data in an array.
      */

    val vertices = Array((1L, ("Charlotte","NY")),(2L,
      ("Yonkers","NY")),(3L, ("East New York","NY")))

    /**
      *Load the array of vertices into the RDD of vertices.
      */

    val verRdd=sc.parallelize(vertices)

    /**
      *Load the edge data in an array.
      */

    val edges = Array(Edge(1L,2L,554.83),Edge(2L,3L,47.94),Edge(3L,
      1L,590.37))

    /**
      *Load the data into the RDD of edges.
      */

    val edgRdd=sc.parallelize(edges)

    /**
      *Create the graph.
      */

    val graph = Graph(verRdd,edgRdd)

    /**
      *Print all the vertices of the graph.
      */

    graph.vertices.collect.foreach(println)

    /**
      *Print all the edges of the graph.
      */

    graph.edges.collect.foreach(println)

    /**
      *Print the edge triplets;
      *Triplet is created by adding source and destination attributes to an edge.
      */

    graph.triplets.collect.foreach(println)

    /**
      *In-degree of a graph is the number of inward-directed edges it has.
      *  Print the in-degree of each vertex.
      */
    val g= graph.inDegrees

    g.foreach(println)
  }

}
