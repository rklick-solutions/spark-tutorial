package com.tutorial.graphX.Rklick_Test

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by ved on 10/3/16.
  */
object Test1 {

  def main(args: Array[String]) {

    val sc = SparkCommon.sparkContext
    val sqlContext = new SQLContext(sc)

    val dataSet = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node_Rklick_Test.csv")

    def getVertices(df: DataFrame): RDD[(Long, (Long, Long, String, String, Long, String, String, String, String, String, String))] = {
      df.map {
        case row => (row.getAs[Any]("id").toString.toLong,
          (row.getAs[Any]("fid").toString.toLong, row.getAs[Any]("lid").toString.toLong, row.getAs[Any]("follow").toString, row.getAs[Any]("name").toString,
            row.getAs[Any]("age").toString.toLong, row.getAs[Any]("location").toString, row.getAs[Any]("specialization").toString, row.getAs[Any]("marital").toString
            , row.getAs[Any]("twitter").toString, row.getAs[Any]("fb").toString, row.getAs[Any]("movie").toString))
      }
    }

    def getEdges(df1: DataFrame): RDD[Edge[String]] = {
      df1.map {
        case row => Edge(row.getAs[Any]("id").toString.toLong,
          row.getAs[Any]("id").toString.toLong, row.getAs[Any]("specialization").toString)
      }
    }

    val vertices = getVertices(dataSet)
    val edges = getEdges(dataSet)

    /**
      * Create Graph.
      */
    val graph = Graph(vertices, edges)
    graph.vertices.collect.foreach(println)
    graph.edges.collect().foreach(println)

    /**
      * 1.Analysis data on basis of specialization
      *
      */
    printSpecialization("all rounder", "batting", "bowler")
    def printSpecialization(specs: String*): Unit = {
      specs.foreach { spec =>
        graph.vertices.filter { case (id, (_, _, _, _, _, _, specialization, _, _, _, _)) =>
          specialization.equals(spec)
        }.collect.foreach { case (id, (_, _, _, name, _, _, _, _, _, _, _)) =>
          println(f"$spec:$id $name")
        }
      }
    }

    /**
      * 2. Analysis based on location
      *
      */
    printLocations("delhi", "mumbai", "chennai", "up", "ranchi")

    def printLocations(locations: String*): Unit = {
      locations.foreach { name =>
        graph.vertices.filter { case (id, (_, _, _, _, _, location, _, _, _, _, _)) =>
          location.equals(name)
        }.collect.foreach(println)
      }
    }

    /**
      * 3.Analysis data on basis of follower
      *
      */
    printFollowers("friend", "best friend", "good friend")

    def printFollowers(followers: String*): Unit = {
      followers.foreach { value =>
        graph.vertices.filter { case (id, (_, _, follow, _, _, _, _, _, _, _, _)) =>
          follow.equals(value)
        }.collect.foreach(println)
      }
    }

    graph.edges.filter { case Edge(from, to, property) =>
      property == "friend" | property == "good friend" | property == "best friend"
    }.collect.foreach(println)

    graph.edges.filter { case Edge(from, to, property) =>
      property == "bowler" | property == "batting" | property == "all rounder"
    }.collect.foreach(println)

    graph.edges.filter { case Edge(from, to, property) =>
      property == "delhi" | property == "up" | property == "chennai" | property == "mumbai" | property == "ranchi"
    }.collect.foreach(println)


    /**
      * 4.Analysis based Age
      *
      */
    graph.vertices.filter { case (id, (_, _, _, name, age, _, _, _, _, _, _)) =>
      age.toLong > 26.toShort
    }.collect.foreach { case (id, (_, _, _, name, age, _, _, _, _, _, _)) =>
      println(f"Younger:$id $name $age")
    }

    graph.vertices.filter { case (id, (_, _, _, name, age, _, _, _, _, _, _)) =>
      age.toLong < 26.toShort
    }.collect.foreach { case (id, (_, _, _, name, age, _, _, _, _, _, _)) =>
      println(f"young :$id $name $age")
    }

    /**
      * 5.Analysis based on marital
      *
      */
    List("Married", "Unmarried").foreach { status =>
      graph.vertices.filter { case (id, (_, _, _, _, _, _, _, marital, _, _, _)) =>
        marital.equals(status)
      }.collect.foreach { case (id, (_, _, _, name, age, _, _, _, _, _, _)) =>
          println(f"$status :$id $name $age")
      }
    }

    /**
      * 5.Analysis based on twitter
      *
      */
    val t1 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      twitter.equals("y")
    }

    t1.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"y :$id $name $age")
    }

    val t2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      twitter.equals("n")
    }

    t2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"n :$id $name $age")
    }

    /**
      * 5.Analysis based on fb
      *
      */
    val f1 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      fb.equals("y")
    }

    f1.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"y :$id $name $age")
    }

    val f2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      fb.equals("n")
    }

    f2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"n :$id $name $age")
    }

    /**
      * 5.Analysis based on movie
      *
      */
    val movie1 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      movie.equals("Hollywood")
    }

    movie1.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"Hollywood :$id $name $age")
    }

    val movie2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      movie.equals("Bollywood")
    }

    movie2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"Bollywood :$id $name $age")
    }

  }

}
