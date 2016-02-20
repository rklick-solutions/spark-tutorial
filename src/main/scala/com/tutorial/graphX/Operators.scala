package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 18/2/16.
  */
object Operators {

  val sc = SparkCommon.sparkContext


  def main(args: Array[String]) {

    /**
      * First we Create an RDD for vertex
      */
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(wrapRefArray
      (Array((3L, ("Bob", "student")),
        (7L, ("ved", "postdoc")),
        (5L, ("Mittal", "prof")),
        (2L, ("Robin", "prof")),
        (4L, ("Amit", "student")))))

    /**
      * Create an RDD for edges.
      */
    val relationships: RDD[Edge[String]] =
      sc.parallelize(wrapRefArray
      (Array(Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),
        Edge(5L, 0L, "colleague"))))

    /**
      * Define a default user in case there are relationship with missing user
      */
    val defaultUser = ("John Doe", "Missing")

    /**
      * Build the initial Graph
      */

    val graph = Graph(users, relationships, defaultUser)

    /**
      * Notice that there is a user 0 (for which we have no information) connected to users
      * 4 (peter) and 5 (franklin).
      */

    graph.triplets.map(
      triplet => triplet.srcAttr._1
        + " is the " + triplet.attr
        + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    /**
      * Remove missing vertices as well as the edges to connected to them
      */


    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    /**
      * The valid subgraph will disconnect users 4 and 5 by removing user 0
      */


    validGraph.vertices.collect.foreach(println(_))

    validGraph.triplets.map(

      triplet => triplet.srcAttr._1
        + " is the " + triplet.attr
        + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))


  }

}
