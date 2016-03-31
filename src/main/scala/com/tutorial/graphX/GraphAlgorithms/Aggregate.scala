package com.tutorial.graphX.GraphAlgorithms

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 29/3/16.
  */
object Aggregate {
  val sc = SparkCommon.sparkContext

  val file_path = "src/main/resources/facebook_edges1.csv"
  val file_path1 = "src/main/resources/facebook_nodes1.csv"


  def main(args: Array[String]) {

    val edgesFile = sc.textFile(file_path)

    val edges = edgesFile.map(_.split(",")).map(e => Edge(e(0).
       toLong,e(1).toLong,e(2)))

    val verticesFile = sc.textFile(file_path1)

    val vertices = verticesFile.map(_.split(",")).map(e =>
      (e(0).toLong, e(1)))
    val graph = Graph(vertices,edges)
    //
    val followerCount = graph.aggregateMessages[(Int)]( t =>
      t.sendToDst(1), (a, b) => (a+b))


    //Print followerCount in the form of (followee, number of followers):
    followerCount.collect.foreach(println)



  }

}
