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


    val verticesDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node_Rklick_Test.csv")


    val selectedData = verticesDf.select("id", "fid", "lid", "follow", "name", "age", "location", "specialization", "marital", "twitter", "fb", "movie")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")

    val edgeDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node_Rklick_Test.csv")
    //.load("src/main/resources/Cricket_Edges1.csv")

    /**
      * based on specialization
      * val selectedData1 = edgeDf.select("id", "id1", "specialization")
      * selectedData1.write
      * .format("com.databricks.spark.csv")
      * .option("header", "true")

      */

    /**
      * based on Location
      */
    val selectedData1 = edgeDf.select("id", "fid", "lid", "follow", "name", "age", "location", "specialization", "marital", "twitter", "fb", "movie")
    selectedData1.write
      .format("com.databricks.spark.csv")
      .option("header", "true")


    // verticesDf.show()
    //edgeDf.show()


    def getVertices(df: DataFrame): RDD[(Long, (Long, Long, String, String, Long, String, String, String, String, String, String))] = {
      df.map {
        case row => (row.getAs[Any]("id").toString.toLong,
          (row.getAs[Any]("fid").toString.toLong, row.getAs[Any]("lid").toString.toLong, row.getAs[Any]("follow").toString, row.getAs[Any]("name").toString,
            row.getAs[Any]("age").toString.toLong, row.getAs[Any]("location").toString, row.getAs[Any]("specialization").toString, row.getAs[Any]("marital").toString
            , row.getAs[Any]("twitter").toString, row.getAs[Any]("fb").toString, row.getAs[Any]("movie").toString))
      }
    }

    /**
      * Based on specialization
      * def getEdges(df1: DataFrame): RDD[Edge[String]] = {
      * df1.map {
      * case row => Edge(row.getAs[Any]("id").toString.toLong,
      * row.getAs[Any]("id1").toString.toLong, row.getAs[Any]("specialization").toString)
      * }
      * }
      */

    /*def getEdges(df1: DataFrame): RDD[Edge[String]] = {
      df1.map {
        case row => Edge(row.getAs[Any]("fid").toString.toLong,
          row.getAs[Any]("id").toString.toLong, row.getAs[Any]("follow").toString)
      }
    }*/
    /*

        def getEdges(df1: DataFrame): RDD[Edge[String]] = {
          df1.map {
            case row => Edge(row.getAs[Any]("id").toString.toLong,
              row.getAs[Any]("lid").toString.toLong, row.getAs[Any]("location").toString)
          }
        }
    */


    def getEdges(df1: DataFrame): RDD[Edge[String]] = {
      df1.map {
        case row => Edge(row.getAs[Any]("id").toString.toLong,
          row.getAs[Any]("id").toString.toLong, row.getAs[Any]("specialization").toString)
      }
    }



    val vertices = getVertices(verticesDf)
    val edges = getEdges(edgeDf)

    /**
      * Create Graph.
      */

    val graph = Graph(vertices, edges)


    //graph.connectedComponents().vertices.collect.foreach(println)
    graph.vertices.collect.foreach(println)
    graph.edges.collect().foreach(println)
    //graph.connectedComponents().edges.collect.foreach(println)

    //graph.collectNeighborIds()
    //graph.vertices.collect().foreach(println)

    //graph.connectedComponents().vertices

    /**
      * val connByAllRounder = vertices.join(connected).map {
      * case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)
      * }
      * val connByBowler = vertices.join(connected).map {
      * case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)

      * }

      * val connByBatting = vertices.join(connected).map {
      * case (id, ((name, age, location, specialization), conn)) => (conn, name, specialization)

      * }

      * connByAllRounder.collect.foreach {
      * case (conn, name, specialization) =>
      * println(f"AllRounder $conn $name $specialization")
      * }
      * connByBatting.collect.foreach {
      * case (conn, name, specialization) =>
      * println(f"Batting $conn $name $specialization")
      * }
      * connByBowler.collect.foreach {
      * case (conn, name, specialization) =>
      * println(f"Bowler$conn $name $specialization")
      * }

      */


    /**
      * 1.Analysis data on basis of specialization
      *
      */

    val s1 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      specialization.equals("all rounder")
    }

    s1.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"all rounder:$id $name")
    }


    val s2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      specialization.equals("batting")
    }

    s2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"batting:$id $name")

    }

    val s3 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      specialization.equals("bowler")
    }


    s3.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"bowler:$id $name")

    }


    /**
      * 2. Analysis based on location
      *
      */

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      location.equals("delhi")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      location.equals("mumbai")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      location.equals("chennai")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      location.equals("up")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      location.equals("ranchi")
    }.collect.foreach(println)

    /*val c = graph.edges.filter { case Edge(from, to, property)
    => property == "friend" | property == "good friend" | property == "best friend"
    }.collect.foreach(println)*/

    /**
      * 3.Analysis data on basis of follower
      *
      */

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      follow.equals("friend")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      follow.equals("best friend")
    }.collect.foreach(println)

    graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      follow.equals("good friend")
    }.collect.foreach(println)


    val c1 = graph.edges.filter { case Edge(from, to, property)
    => property == "friend" | property == "good friend" | property == "best friend"
    }.collect.foreach(println)

    val c2 = graph.edges.filter { case Edge(from, to, property)
    => property == "bowler" | property == "batting" | property == "all rounder"
    }.collect.foreach(println)

    val c3 = graph.edges.filter { case Edge(from, to, property)
    => property == "delhi" | property == "up" | property == "chennai" | property == "mumbai" | property == "ranchi"
    }.collect.foreach(println)


    /**
      * 4.Analysis based Age
      *
      */


    val c4 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      age.toLong > 26.toShort
    }

    c4.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"Younger:$id $name $age")


    }

    val c5 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      age.toLong < 26.toShort
    }

    c5.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"young :$id $name $age")
    }

    /**
      * 5.Analysis based on marital
      *
      */
    val m1 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      marital.equals("Married")
    }

    m1.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"Married :$id $name $age")
    }

    val m2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      marital.equals("Unmarried")
    }

    m2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"unmarried :$id $name $age")
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
        println(f"Hollywood :$id $name $age")  }




    val movie2 = graph.vertices.filter { case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
      movie.equals("Bollywood")
    }

    movie2.collect.foreach {
      case (id, (fid, lid, follow, name, age, location, specialization, marital, twitter, fb, movie)) =>
        println(f"Bollywood :$id $name $age")
    }


  }
}
