package com.tutorial.graphX.Rklick_Test

import com.tutorial.utils.SparkCommon
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable


/**
  * Created by ved on 28/3/16.
  */
object Test3 {


  /*{
  val sc = SparkCommon.sparkContext
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._


  def main(args: Array[String]) {




    val verticesDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node_Rklick_Test.csv")
      /*.groupBy("location").agg(count("name"))
    verticesDf.take(5).foreach(println)*/









    //val df2= verticesDf.groupBy('id', 'C1', 'C2').agg({'C3': 'sum'}).sort('id', 'C1')
       // val df2= verticesDf.count()
    //println(df2.count())
   // println(" count:" + verticesDf.count())

           // .groupBy("id","lid",  "name", "movie")

            //.agg(sum("id")).sort("lid","name").show()
            //.groupBy("id",  "name", "movie")
            //.agg(sum("movie")).count()

           //. groupBy("name").agg(max("age"), sum("age")).show()



      //.agg("",)
      //.aggregateByKey(0)(_+_,_+_)
      /*val inputFile = sc.textFile("src/main/resources/Cricket_Node.csv")
      val count1 = inputFile.flatMap(line => line.split(","))
      .map(word => (word, 1)).reduceByKey(_ + _)

       println(" agg11:" + count1.collect().mkString(","))

    val df2= count1.groupBy("name","id").agg(max("age")*/

    //count1.collect().mkString(",")foreach(println)










  }

}*/


  def main(args: Array[String]) {


    val sc = SparkCommon.sparkContext
    val sqlContext = new SQLContext(sc)

  /*  val keysWithValuesList = Array("sachin=mumbai", "rohit=mumbai", "virat=delhi", "sehwag=delhi",
      "sachin=batsman", "rohit=batsman", "virat=batsman", "sehwag=bollwer")

    /*val verticesDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/Cricket_Node.csv")*/
    //val data = sc.parallelize(verticesDf)

    val data = sc.parallelize(keysWithValuesList)

    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)



    println("Aggregate By Key unique Results")

    val uniqueResults = uniqueByKey.collect()
    for (indx <- uniqueResults.indices) {
      val r = uniqueResults(indx)
      println(r._1 + " -> " + r._2.mkString(","))
    }

    println("------------------")

    println("Aggregate By Key sum Results")
    val sumResults = countByKey.collect()
    for (indx <- sumResults.indices) {
      val r = sumResults(indx)
      println(r._1 + " -> " + r._2)
    }

    def coursePos (tag: String) = tag match {
      case "NN" | "NNS" | "NNP" | "NNPS"                       => "Noun"
      case "JJ" | "JJR" | "JJS"                                => "Adjective"
      case _                                                   => "Other"
    }

    r.map(coursePos)*/

    import sqlContext.implicits._

    // create an RDD with some data
    val players = Seq(
      Players(1, "Scahin", "Mumbai", "Bating",100,50, 40),
      Players(2, "shehwag", "Delhi", "Allrounder",40,30, 38),
      Players(3, "virat", "Delhi", "Bating", 30,40,25),
      Players(4, "raina", "Up", "Allrounder",24,45, 28),
      Players(5, "youraj", "Chandigarh", "Allrounder",24,60, 30),
      Players(6, "Dhoni", "Ranchi", "Bating", 44,65,40),
      Players(7, "Ashwin", "Banglore", "Allrounder", 4,5,38),
      Players(8, "surabh", "Kolkatta", "Bating", 24,45,25),
      Players(9, "Rohit", "Up", "Allrounder",34,45, 28),
      Players(10, "Bhajji", "Chandigarh", "Allrounder",24,25, 30)
    )
    val customerTable = sc.parallelize(players).toDF()

    // DSL usage -- query using a UDF but without SQL
    // (this example has been repalced by the one in dataframe.UDF)

    def westernState(location: String) = Seq("Mumbai", "Up", "Delhi", "Chandigarh","Ranchi","Banglore","Kolkatta").contains(location)

    // for SQL usage  we need to register the table

    customerTable.registerTempTable("customerTable")

    // WHERE clause

    sqlContext.udf.register("westernState", westernState _)

    println("UDF in a WHERE")
    val westernStates =
      sqlContext.sql("SELECT * FROM customerTable WHERE westernState(location)")
    westernStates.foreach(println)

    // HAVING clause

    def manyCustomers(cnt: Long) = cnt > 2

    sqlContext.udf.register("manyCustomers", manyCustomers _)

    println("UDF in a HAVING")
    val statesManyCustomers =
      sqlContext.sql(
        s"""
           |SELECT location, COUNT(id) AS custCount
           |FROM customerTable
           |GROUP BY location
           |HAVING manyCustomers(custCount)
         """.stripMargin)
    statesManyCustomers.foreach(println)

    // GROUP BY clause

    def stateRegion(location:String) = location match {
      case "Chandigarh" | "Up" | "Mumbai" | "Delhi"|"Ranchi"|"Banglore"|"Kolkatta" => "India"

    }

    sqlContext.udf.register("stateRegion", stateRegion _)

    println("UDF in a GROUP BY")
    // note the grouping column repeated since it doesn't have an alias
    val salesByRegion =
      sqlContext.sql(
        s"""
           |SELECT SUM(Hundred), stateRegion(location) AS totalRuns
           |FROM customerTable
           |GROUP BY stateRegion(location)
        """.stripMargin)
    salesByRegion.foreach(println)

    // we can also apply a UDF to the result columns

    def discountRatio(Hundred: Int, Fifty: Int) = Hundred/Fifty

    sqlContext.udf.register("discountRatio", discountRatio _)

    println("UDF in a result")
    val customerDiscounts =
      sqlContext.sql(
        s"""
           |SELECT name, discountRatio(Hundred, Fifty) AS ratio
           |FROM customerTable
        """.stripMargin)
    customerDiscounts.foreach(println)

    // we can make the UDF create nested structure in the results


    def makeStruct(Hundred: Int, Fifty:Int) = RunsDisc(Hundred, Fifty)

    sqlContext.udf.register("makeStruct", makeStruct _)

    // these failed in Spark 1.3.0 -- reported SPARK-6054 -- but work again in 1.3.1

    println("UDF creating structured result")
    val withStruct =
      sqlContext.sql("SELECT makeStruct(Hundred, Fifty) AS sd FROM customerTable")
    withStruct.foreach(println)

    println("UDF with nested query creating structured result")
    val nestedStruct =
      sqlContext.sql("SELECT id, sd.Hundred FROM (SELECT id, makeStruct(Hundred, Fifty) AS sd FROM customerTable) AS d")
    nestedStruct.foreach(println)
  }



}

// a case class for our sample table
case class Players(id: Integer, name: String, location : String, specialization:String,Hundred:Int,Fifty:Int, age: Int)
case class RunsDisc(Hundred: Int, Fifty: Int)


