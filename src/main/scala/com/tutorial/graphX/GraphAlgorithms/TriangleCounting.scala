package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Triangle
  * A vertex is part of a triangle when it has two adjacent vertices with an edge between them.
  * GraphX implements a triangle counting algorithm in the TriangleCount object that determines
  * the number of triangles passing through each vertex, providing a measure of clustering.
  * Created by ved on 21/2/16.
  */
object TriangleCounting {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * Load the edges in canonical order and partition the graph for triangle count
      */

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt", true).
      partitionBy(PartitionStrategy.RandomVertexCut)

    /**
      * Find the triangle count for each vertex
      *
      */

    val triCounts = graph.triangleCount().vertices

    /**
      * Join the triangle counts with the usernames
      *
      */

    val users = sc.textFile("src/main/resources/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }

    /**
      * Print the result
      *
      */
    println(triCountByUsername.collect().mkString("\n"))


  }

}
