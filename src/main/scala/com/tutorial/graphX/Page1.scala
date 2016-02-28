package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 19/2/16.
  */
object Page1 {
  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {


    val PATH_ADD = "src/main/resources/followers.txt"


    /**
      * Load the graph as in the PageRank example
      *
      */

    val graph = GraphLoader.edgeListFile(sc, PATH_ADD)


    /**
      * Find the connected components
      */
    val cc = graph.connectedComponents().vertices

    /**
      * Join the connected components with the usernames
      */
    val users = sc.textFile(PATH_ADD).map { line =>
      val fields = line.split(" ")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }

    /**
      * Print the result
      */
    println(ccByUsername.collect().mkString("\n"))


  }

}
