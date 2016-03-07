package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 21/2/16.
  */
object GraphExample {

  val sc = SparkCommon.sparkContext


  def main(args: Array[String]) {


    /**
      * Load my user data and parse into tuples of user id and attribute list
      */

    //val users = sc.textFile("src/main/resources/users.txt")
    //  .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail))

    val users = sc.textFile("src/main/resources/users1.txt")
     .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail))

    /**
      * Parse the edge data which is already in userId -> userId format.
      */


   // val followerGraph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt")

    val followerGraph = GraphLoader.edgeListFile(sc, "src/main/resources/followers1.txt")
    /**
      * Attach the user attributes.
      */

    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList

      /**
        * Some users may not have attributes so we set them as empty.
        */

      case (uid, deg, None) => Array.empty[String]
    }

    /**
      * Restrict the graph to users with usernames and names.
      */

    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    /**
      * Compute the PageRank
      */
    val pagerankGraph = subgraph.pageRank(0.001)

    /**
      * Get the attributes of the top pagerank users
      */


    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }


    /**
      * print top 5 rank.
      */
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))


  }

}
