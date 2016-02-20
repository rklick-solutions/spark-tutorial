package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._

/**
  * Created by ved on 18/2/16.
  */
object PageRank {

  val sc = SparkCommon.sparkContext


  def main(args: Array[String]) {

    val edgesFile = sc.textFile("src/main/resources/followers.txt",20)

    val edges = edgesFile.flatMap { line =>
      val links = line.split("\\W+")
      val from = links(0)
      val to = links.tail
      for ( link <- to) yield (from,link)
    }.map( e => Edge(e._1.toLong,e._2.toLong,1))

    val verticesFile = sc.textFile("src/main/resources/users.txt",20)
    

    val vertices = verticesFile.zipWithIndex.map(_.swap)

    val graph = Graph(vertices,edges)
    val ranks = graph.pageRank(0.001).vertices
    val swappedRanks = ranks.map(_.swap)
    val sortedRanks = swappedRanks.sortByKey(false)
    val highest = sortedRanks.first
    val join = sortedRanks.leftOuterJoin(vertices)

   val final = join.map ( v => (v._2._1, (v._1,v._2._2))).
     sortByKey(false)
    final.take(5).collect.foreach(println)



  }




}
