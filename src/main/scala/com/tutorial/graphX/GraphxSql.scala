package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 5/3/16.
  */
object GraphxSql {

  def main(args: Array[String]) {

    val sc = SparkCommon.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext._



    val vertexArray = Array(
      (1L, Peep("Kim", 23)),
      (2L, Peep("Pat", 31)),
      (3L, Peep("Chris", 52)),
      (4L, Peep("Kelly", 39)),
      (5L, Peep("Leslie", 45))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 5L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 3L, 9)
    )

    val vertexRDD: RDD[(Long, Peep)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val g: Graph[Peep, Int] = Graph(vertexRDD, edgeRDD)

    val results = g.triplets.filter(t => t.attr > 7)

    for (triplet <- results.collect) {
      println(s"${triplet.srcAttr.name} loves ${triplet.dstAttr.name}")

     // val word = sqlContext.sql("word.parquet")
     // word.registerTempTable("word")

     // val edge = sql("edge.parquet")
      //edge.registerTempTable("edge")

      //sql("SELECT * FROM Peep").take(5)
      //sql("SELECT * FROM edge").take(5)

      val n = sql("SELECT id, stem FROM word").distinct()
      val nodes: RDD[(Long, String)] = n.map(p => (p(0).asInstanceOf[Long], p(1).asInstanceOf[String]))

      val e = sql("SELECT * FROM edge")
      val edges: RDD[Edge[Int]] = e.map(p => Edge(p(0).asInstanceOf[Long], p(1).asInstanceOf[Long], 0))

      val g: Graph[String, Int] = Graph(nodes, edges)

      val ranks = g.pageRank(0.0001).vertices
      ranks.join(nodes).sortBy(_._2._1, ascending = false).foreach(println)


    }
  }
}
case class Peep(name: String, age: Int)