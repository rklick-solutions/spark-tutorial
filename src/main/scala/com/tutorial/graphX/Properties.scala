package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 19/2/16.
  */
object Properties {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {


    val vertexArray = Array(
      (1L, ("Sachin", 38)),
      (2L, ("shehwag", 37)),
      (3L, ("Dravid", 35)),
      (4L, ("kohali", 22)),
      (5L, ("Dhoni", 35)),
      (6L, ("Raina", 30)),
      (7L, ("Rohit", 28)),
      (8L, ("Ashwin", 31)),
      (9L, ("Zair", 35)),
      (10L, ("Irfan", 32)),
      (11L, ("Nehra", 34))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(6L, 3L, 8),
      Edge(7L, 6L, 3),
      Edge(8L, 2L, 2),
      Edge(9L, 3L, 8),
      Edge(10L, 6L, 3),
      Edge(11L, 6L, 3)
    )

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    graph.vertices.collect.foreach(println)


    graph.vertices.filter { case (id, (name, age)) => age > 20 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")


    }

  }
}
