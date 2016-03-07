package com.tutorial.graphX.GraphAlgorithms

import org.apache.spark.graphx._

import com.tutorial.utils.SparkCommon
import org.apache.spark.rdd.RDD

/**
  * Created by ved on 28/2/16.
  */
object ConnectedGraph {

  val sc = SparkCommon.sparkContext

  //val file_path = "src/main/resources/facebook_edges.csv"
  //val file_path1 = "src/main/resources/facebook_nodes.csv"

  val file_path = "src/main/resources/relationedge.csv"
  val file_path1 = "src/main/resources/relationnode.csv"

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
    val ccEdges =cc.edges
    //print("connected vertices" )
    //ccVertices.collect.foreach(println )
    println("connected vertices" + ccVertices.collect().mkString(","))
    println("connected edges" + ccEdges.collect().mkString(","))
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
