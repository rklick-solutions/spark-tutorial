package com.tutorial.sparkcore

import java.util.UUID

import com.tutorial.utils.SparkCommon

/**
  * An example  of two types of operations: transformations and actions.
  * Created by ved on 7/1/16.
  */
object RDDOperations {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * RDD operation for  Word count .
      */
    val inputFile = sc.textFile("src/main/resources/phone_data.csv") // Load our input data.
    val count1 = inputFile.flatMap(line => line.split(" ")) // Split it up into words.
        .map(word => (word, 1)).aggregateByKey(0)(_+_,_+_) // Transform into pairs and count.
    println(" agg11:" + count1.collect().mkString(","))

    //Save the word count back out to a text file, causing evaluation.
    //count.saveAsTextFile(s"src/main/resources/${UUID.randomUUID()}")
    //println("OK")

    /**
      * Operations on RDDs.
      * count() returns a count of the elements the RDD.
      */
    val inputRDD = sc.textFile("src/main/resources/test_file.txt")

    val dataRDD = inputRDD.filter(line => line.contains("data"))
    println(dataRDD.count())

    /**
      * The most common action on basic RDDs
      * reduce(func) Combine the elements of the RDD together in parallel (e.g., sum ).
      */
    val x = sc.parallelize(List(1, 2, 4, 4))
    val y = sc.parallelize(List(6, 5, 1, 4))

    val rUnion = x.union(y)
    val resultReduce = rUnion.reduce((x, y) => x + y)
    println("reduce:" + resultReduce + " ")

    /**
      * Basic actions on an RDD containing {1, 2, 3, 3}
      * count() returns a count of the elements the RDD.
      */
    val inputCount = sc.parallelize(List(1, 2, 4, 4))
    println(" count:" + inputCount.count())

    /**
      * example of map() that squares all of the numbers in an RDD
      */
    val inputNumbers = sc.parallelize(List(1, 2, 3, 4))
    val resultSquare = inputNumbers.map(x => x * x)
    println("Square:" + resultSquare.collect().mkString(","))

    /**
      * agg
      */

    //val pairs = sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val pairs = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)))
    val resAgg = pairs.aggregateByKey(0)(_+_,_+_)
    println(" agg:" + resAgg.collect().mkString(","))



  }
}

