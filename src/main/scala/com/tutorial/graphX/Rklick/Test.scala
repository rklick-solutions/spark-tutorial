package com.tutorial.graphX.Rklick

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by ved on 9/3/16.
  */
object Test {

  val sc = SparkCommon.sparkContext


  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]) {

    //val file_path1 = "src/main/resources/Cricket_Node.csv"
    // val file_path2 = "src/main/resources/Cricket_Edges.csv"

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
      .load("src/main/resources/Cricket_Node.csv")

    val selectedData1 = edgeDf.select("id", "name", "age", "location", "specialization")
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
          row.getAs[Any]("age").toString.toLong, row.getAs[Any]("location").toString)
      }
    }


    val vertices = getVertices(verticesDf)
    val edges = getEdges(edgeDf)

    /**
      * Create Graph.
      */

    val graph = Graph(vertices, edges)

    /**
      * Connected Vertices Count
      *
      */
    val connected_vertices = vertices.count()
    println("connected vertices" + connected_vertices)

    /**
      * Connected Edges Count
      *
      */
    val connected_edges = edges.count()
    println("connected edges" + connected_edges)


    /**
      * Connected Component.
      */

    val cc = graph.connectedComponents()

    /**
      * No of Vertices show
      */
    val ccVertices = cc.vertices
    println("connected vertices" + ccVertices.collect.mkString("\n"))

    /**
      * No of Edges show
      */
    val ccEdges = cc.edges
    println("connected edges" + ccEdges.collect.mkString("\n"))

    //page rank

   // val c1 = graph.vertices.filter { case (id, (name, age, location, specialization)) => age.
   //   toLong > 20
  //  }.count


    // val c2 = graph.edges.collect().foreach(println)
   // val c2 = graph.edges.filter { case Edge(src, to, dest)
   // => dest == "delhi" | dest == "up"
  //  }.count
  //  println("Vertices count : " + c1)
   // println("Edges count : " + c2)

    val startTime = System.currentTimeMillis()
    val tolerance = 0.0001
    val ranking = graph.pageRank(tolerance).vertices
    val rankByPerson = vertices.join(ranking).map {
      case (id, (name, rank)) => (rank, id, name)
    }

    rankByPerson.collect().foreach {
      case (rank, id, name) => println(f"Rank $rank%1.2f id $id name $name")
    }

    //println(s"Taking time::::::::${System.currentTimeMillis() - startTime}")

    //val finalAll = rankByPerson.sortBy(v => (v._1, v._2, v._2 )).foreach(println)
    //sortByKey(false)


    val startTime1 = System.currentTimeMillis()
    val finalAll = rankByPerson.sortBy(v => (v._1, v._2, v._2)).foreach(println)
    //println("top five ranked pages" + finalAll.collect().take(5).mkString("\n"))
    //finalAll.collect().take(5).foreach(println)
    println(s"Taking time::::::::${System.currentTimeMillis() - startTime1}")

    // traingle count
    val tCount = graph.triangleCount().vertices
    println(tCount.collect().mkString("\n"))

    //connected
    val iterations = 1000
    val connected = graph.connectedComponents().vertices
    val connectedS = graph.stronglyConnectedComponents(iterations).vertices

    val connByName = vertices.join(connected).map {
      case (id, ((name, age, location, specialization), conn)) => (conn, name, age)
    }
    val connByName1 = vertices.join(connectedS).map {
      case (id, ((name, age, location, specialization), conn)) => (conn, name, age)
    }

    connByName.collect().foreach {
      case (conn, name, age) =>
        println(f"Weak $conn $name $age")
    }

    connByName1.collect().foreach {
      case (conn, name, age) =>
        println(f"Strong $conn $name $age")
    }
    verticesDf.registerTempTable("Person")

    val query = s"""select * from (select age,name from Person group by age,name having count(*) > 1) Person"""
    val result1 = sqlContext.sql(query)

    println("Filter data:" + result1.collect.mkString(","))


  }
}
