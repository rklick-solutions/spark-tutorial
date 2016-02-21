package com.tutorial.graphX
import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 19/2/16.
  */
object Page1 {
  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * Load the graph as in the PageRank example
      *
      */

    val graph = GraphLoader.edgeListFile(sc,"src/main/resources/followers.txt")


    /**
      * Find the connected components
      */
    val cc = graph.connectedComponents().vertices

    /**
      * Join the connected components with the usernames
      */
    val users = sc.textFile("src/main/resources/users.txt").map { line =>
      val fields = line.split(",")
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
