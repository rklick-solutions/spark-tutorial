package com.tutorial.graphX


import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD

//import org.graphframes.examples

case class Relat(id: Int, name: String, age: Int, city: String, country: String)

//case Relat1()
/**
  * Created by ved on 4/3/16.
  */
object Relation {

  val sc = SparkCommon.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    /**
      * First we Load the vertex data in an array:
      */

    val vertexArray = Array(

      (1L, Relat(1, "Ved", 26, "Delhi", "India")),
      (2L, Relat(2, "Himanshu", 24, "Agra", "India")),
      (3L, Relat(3, "Amit", 25, "Lucknow", "India")),
      (4L, Relat(4, "Sumit", 32, "Delhi", "India")),
      (5L, Relat(5, "Sandy", 25, "Agra", "India")),
      (6L, Relat(6, "Puneet", 25, "Lucknow", "India")),
      (7L, Relat(7, "Md", 25, "Lucknow", "India"))
    )


    /**
      * Load the array of vertices into the RDD of vertices:
      *
      */

    val vertexRDD: RDD[(Long, Relat)] = sc.parallelize(vertexArray)



    /**
      *
      * Load the edge data in an array:
      */
    val edgeArray = Array(
      Edge(1L, 2L, "friend"),
      Edge(2L, 3L, "follow"),
      Edge(3L, 2L, "follow"),
      Edge(6L, 3L, "follow"),
      Edge(5L, 6L, "follow"),
      Edge(5L, 4L, "friend"),
      Edge(4L, 1L, "friend"),
      Edge(1L, 5L, "friend")
    )

    /**
      * Load the data into the RDD of edges:
      */

    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)

    /**
      * Create the graph:
      */

    val graph: Graph[Relat, String] = Graph(vertexRDD, edgeRDD)




    /**
      * Print all the vertices of the graph:
      */

    graph.vertices.collect.foreach(println)


    graph.edges.collect().foreach(println)

    graph.inDegrees.collect().foreach(println)

    graph.outDegrees.collect().foreach(println)

    graph.degrees.collect().foreach(println)

    //graph.filter("age").collect().groupBy("age").(println)


  }

}
