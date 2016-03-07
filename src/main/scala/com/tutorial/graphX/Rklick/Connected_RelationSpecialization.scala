package com.tutorial.graphX.Rklick

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by ved on 7/3/16.
  */
object Connected_RelationSpecialization {

  def main(args: Array[String]) {

    val sc = SparkCommon.sparkContext
    val sqlContext = new SQLContext(sc)


    val verticesDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node.csv")


    val selectedData = verticesDf.select("id", "name", "age", "location", "specialization")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")

    val edgeDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Edges1.csv")

    val selectedData1 = edgeDf.select("id", "id1", "specialization")
    selectedData1.write
      .format("com.databricks.spark.csv")
      .option("header", "true")

    // verticesDf.show()
    //edgeDf.show()


    def getVertices(df: DataFrame): RDD[(Long, (String, Long, String, String))] = {
      df.map {
        case row => (row.getAs[Any]("id").toString.toLong,
          (row.getAs[Any]("name").toString, row.getAs[Any]("age").toString.toLong,
            row.getAs[Any]("location").toString, row.getAs[Any]("specialization").toString))
      }
    }

    def getEdges(df1: DataFrame): RDD[Edge[String]] = {
      df1.map {
        case row => Edge(row.getAs[Any]("id").toString.toLong,
          row.getAs[Any]("id1").toString.toLong, row.getAs[Any]("specialization").toString)
      }
    }


    val vertices = getVertices(verticesDf)
    val edges = getEdges(edgeDf)
    /**
      * Create Graph.
      */

    val graph = Graph(vertices, edges)


    graph.connectedComponents().vertices.collect.foreach(println)
    graph.connectedComponents().edges.collect.foreach(println)

    //graph.collectNeighborIds()
    //graph.vertices.collect().foreach(println)

    graph.connectedComponents().vertices

  /**  val connByAllRounder = vertices.join(connected).map {
      case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)
    }
    val connByBowler = vertices.join(connected).map {
      case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)

    }

    val connByBatting = vertices.join(connected).map {
      case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)

    }

    connByAllRounder.collect.foreach {
      case (conn, name, specialization) =>
        println(f"AllRounder $conn $name $specialization")
    }
    connByBatting.collect.foreach {
      case (conn, name, specialization) =>
        println(f"Batting $conn $name $specialization")
    }
    connByBowler.collect.foreach {
      case (conn, name, specialization) =>
        println(f"Bowler$conn $name $specialization")
    }

    */

    graph.vertices.filter { case (id, (name, age, location, specialization)) =>
      specialization.equals("all rounder")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (name, age, location, specialization)) =>
      specialization.equals("batting")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (name, age, location, specialization)) =>
      specialization.equals("bowler")
    }.collect.foreach(println)



  }
}

