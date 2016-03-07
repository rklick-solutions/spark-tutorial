package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 20/2/16.
  */
object PageRank1 {

  val sc = SparkCommon.sparkContext

  val file_path = "src/main/resources/followers.txt"
  val file_path1 = "src/main/resources/users.txt"

  //val file_path = "src/main/resources/relationedge.csv"
  //val file_path1 = "src/main/resources/relationnode.csv"

  def main(args: Array[String]) {

    /**
      * Load the edges as a graph
      */

    val graph = GraphLoader.edgeListFile(sc, file_path)

    /**
      * Run PageRank
      */

    val startTime = System.currentTimeMillis()
    val ranks = graph.pageRank(0.0001).vertices
    println(s"Taking time::::::::${System.currentTimeMillis() - startTime}")

    /**
      * Join the ranks with the usernames
      */

    val users = sc.textFile(file_path1).map { line =>
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
