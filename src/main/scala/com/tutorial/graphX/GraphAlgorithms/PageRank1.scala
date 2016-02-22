package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 20/2/16.
  */
object PageRank1 {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * Load the edges as a graph
      */

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt")

    /**
      * Run PageRank
      */

    val ranks = graph.pageRank(0.0001).vertices

    /**
      * Join the ranks with the usernames
      */

    val users = sc.textFile("src/main/resources/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    /**
      * Print the result
      */

    println(ranksByUsername.collect().mkString("\n"))


  }

}
