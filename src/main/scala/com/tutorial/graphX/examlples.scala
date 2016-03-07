package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{SQLContext, Row}

/**
  * Created by ved on 5/3/16.
  */
object examlples {

  val sc = SparkCommon.sparkContext
  val sqlContext = new SQLContext(sc)

  import sqlContext._

  def main(args: Array[String]) {


    /**
      * Next you'll need to get your data registered as a table.
      * Note that in order to infer the schema (column names and types) it must be a case class.
      * A normal class won't work, unless you manually implement much of what a case class would do,
      * e.g. extend Product and Serializable.

      * Any data source can be used, as long as you can transform it to an rdd of case classes.  For now, we'll just use some dummy data:
      */


    val people = sc.parallelize(Array(
      Person("adam", 19, Seq("123-5767", "234-1544")),
      Person("bob", 23, Seq()),
      Person("cathy", 14, Seq("454-1222", "433-1212", "345-2343")),
      Person("dave", 44, Seq("454-1222"))
    ))

    //people.registerTempTable("Persons")


    val likes = sc.parallelize(Array(
      Likes("adam", "cathy"),
      Likes("bob", "dave"),
      Likes("bob", "adam"),
      Likes("cathy", "adam"),
      Likes("cathy", "bob")
    ))



    def id(s: String) = s.head.toLong

    val vertices: RDD[(VertexId, String)] = sql("select name from people").map {
      case Row(name: String) =>
        id(name) -> name
    }

    /*
    helper to make vertex ids more readable
    */

    val nameOf = vertices.collect.toMap

    /*
    GraphX edges are directional, so if you want the equivalent of an undirected graph, you need to make edges in both directions
    Edges are assumed to have a label of some kind (e.g. a weight or a name), if you don't need it, just use 0
    */

    val edges: RDD[Edge[Int]] = sql("select name, likes from likes").map {
      case Row(name: String, likes: String) =>
        Edge(id(name), id(likes), 0)
    }

    /*
    Passing a default avoids errors when an edge refers to an unknown vertex
    */

    val default = "UNKNOWN"

    val graph = Graph(vertices, edges, default)

    /*
    Now that you have a graph object, you can perform operations on it:
    */

    val degrees = graph.degrees.collect.sortWith((a, b) => (a._2 > b._2))

    println(
      s"""
the vertex with the highest degree is "${nameOf(degrees(0)._1)}",
having ${degrees(0)._2} relationships
""")

    val scc = graph.stronglyConnectedComponents(numIter = 5)

    scc.vertices.map {
      case (member, leader) => s"${nameOf(member)} is in ${nameOf(leader)}'s clique"
    }.collect.foreach(println)


  }

}

case class Person(name: String, age: Int, phones: Seq[String])

case class Likes(name: String, likes: String)
