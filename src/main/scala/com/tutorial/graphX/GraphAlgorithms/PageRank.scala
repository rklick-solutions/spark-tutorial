package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._


/**
  * Created by ved on 20/2/16.
  */
object PageRank {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {


    /**
      * Load the edges from file
      * with 20 partitions:
      *
      */
    val edgesFile = sc.textFile("src/main/resources/twitter_followers.txt", 20)

    /**
      * Flatten and convert it into an RDD
      * of "link1,link2" format and then convert it into an RDD of Edge objects:
      */

    val edges = edgesFile.flatMap { line =>
      val links = line.split("\\W+")
      val from = links(0)
      val to = links.tail
      for (link <- to) yield (from, link)
    }.map(e => Edge(e._1.toLong, e._2.toLong, 1))

    /**
      * Load the vertices from file  with 20 partitions:
      *
      */

    val verticesFile = sc.textFile("src/main/resources/twitter_users.txt", 20)
    /**
      * Provide an index to the vertices and then swap it to make it in the (index, title) format:
      */

    val vertices = verticesFile.zipWithIndex.map(_.swap)

    /**
      * Create the graph object:
      *
      */

    val graph = Graph(vertices, edges)

    /**
      * Run PageRank and get the vertices:
      */
    val ranks = graph.pageRank(0.001).vertices

    ranks.foreach(println)

    /**
      * As ranks is in the (vertex ID, pagerank) format,
      * swap it to make it in the (pagerank,vertex ID) format:
      */

    val swappedRanks = ranks.map(_.swap)


    swappedRanks.foreach(println)


    /**
      * Sort to get the highest ranked pages first:
      */

    val sortedRanks = swappedRanks.sortByKey(false)


    sortedRanks.foreach(println)

    /**
      * Get the highest ranked page:
      */

    val highest = sortedRanks.first

    highest.toString().mkString(" ").foreach(println)

    /**
      * The preceding command gives the vertex id,
      * which you still have to look up to see the actual title with rank. Let's do a join:
      */

    val allJoin = ranks.join(vertices)


    /**
      * Sort the joined RDD again after converting from the
      * (vertex ID, (page rank, title))format to the (page rank, (vertex ID, title)) format:
      */
    val finalAll = allJoin.map(v => (v._2._1, (v._1, v._2._2))).
      sortByKey(false)

    /**
      * Print the top five ranked pages
      */

    finalAll.collect().take(5).foreach(println)
    //allJoin.take(5).collect.foreach(println)


  }

}
