package com.tutorial.graphX

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 2/3/16.
  */
object Relationship {

  val sc = SparkCommon.sparkContext


  //def main(args: Array[String]) {

  //val file_path = "src/main/resources/relationship.csv"


  // val edgesFile = sc.textFile(file_path)

  //val edges = edgesFile.map(_.split(",")).map(e => Edge(e(0).
  // toLong,e(1).toLong,e(2)))

  // edges.foreach(println)

  // val vertxText = sc.textFile("users1.txt")
  // val edgesText = sc.textFile("followers1.txt")

  val file_path1 = "src/main/resources/users1.txt"
  val file_path = "src/main/resources/followers1.txt"

  def main(args: Array[String]) {

    /** val edgesFile = sc.textFile(file_path)

      * val edges = edgesFile.map(_.split(",")).map(e => Edge(e(0).
      * toLong,e(1).toLong,e(2)))
      */
    val vertices = getVertices(file_path1)
    val edges = getEdges(file_path)

    val graph = Graph(vertices, edges)
    val cc = graph.connectedComponents

    val ccVertices = cc.vertices
    ccVertices.collect.foreach(println)
  }

  def getVertices(file_path1: String): RDD[(Long, String)] = {
    val verticesFile = sc.textFile(file_path1)

    val vertices = verticesFile.map(_.split(",")).map(e =>
      (e(0).toLong, e(1)))
    vertices
  }

  def getEdges(file_path: String): RDD[Edge[String]] = {


    val edgesFile = sc.textFile(file_path)

    val edges = edgesFile.map(_.split(",")).map(e => Edge(e(0).
      toLong, e(1).toLong, e(2)))
    edges


  }


}
