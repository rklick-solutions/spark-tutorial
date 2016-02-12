package com.tutorial.sparkcore

import java.util.UUID

import com.tutorial.utils.SparkCommon

/**
  *  Actions are operations that return a result to the driver program or write it to storage
  * Created by ved on 8/1/16.
  */
object RDDAction {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {
    /**
      * The most common action on basic RDDs
      * reduce(func) Combine the elements of the RDD together in parallel.
      */
    val input = sc.parallelize(List(3, 2, 4, 6))
    val inputs = sc.parallelize(List(2, 4, 2, 3))

    val rUnion = input.union(inputs)
    val resultReduce = rUnion.reduce((x, y) => x + y)
    println("reduce:" + resultReduce + " ")

    /**
      * The most common action on basic RDDs
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * collect() Return all elements from the RDD.
      */
    val inputElement = sc.parallelize(List(2, 3, 4, 4))
    println("collect" + inputElement.collect().mkString(","))

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * count() returns a count of the elements the RDD.
      */
    val inputCount = sc.parallelize(List(2, 3, 4, 4))
    println(" count:" + inputCount.count())

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * countByValue()  returns Number of times each element occurs in the RDD  .
      */
    val inputCountByValue = sc.parallelize(List(2, 3, 4, 4))
    println("countByValue :" + inputCountByValue.countByValue().mkString(","))

    /**
      * first()
      * Returns the first element of the dataset (similar to take (1)).
      */
    val inputFirst = sc.parallelize(List(2, 3, 4, 4))
    println("fist:" + inputFirst.first())

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * take(num) Return num elements from the RDD.
      */
    val inputTake = sc.parallelize(List(2, 3, 4, 4))
    println("take :" + inputTake.take(2).mkString(","))

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * top(num) Return the top num elements the RDD.
      */
    val inputTop = sc.parallelize(List(2, 3, 4, 4))
    println("Top:" + inputTop.top(2).mkString(","))

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * takeOrdered(num)(ordering) Return num elements based on provided ordering
      */
    val inputOrder = sc.parallelize(List(2, 3, 4, 4))
    println("Take Order :" + inputOrder.takeOrdered(2).mkString(","))

    /**
      * foreach()
      * Runs a function func on each element of the dataset.
      */
    val inputForeach = sc.parallelize(List(2, 3, 4, 4))
    inputForeach.foreach((x => println(x + 1)))

    /**
      * example of map() that squares all of the numbers in an RDD
      */
    val inputNumbers = sc.parallelize(List(2, 3, 4, 4))
    val resultSquare = inputNumbers.map(x => x * x)
    println("Square:" + resultSquare.collect().mkString(","))

    /**
      * Actions are available on pair RDDs
      * Actions on pair RDDs (example ({(1, 2), (2, 3), (5, 4)}))
      * countByKey() Count the number of elements for each key.
      */
    val inputAction = sc.parallelize(List((1, 2), (2, 3), (5, 4)))
    println("countByKey :" + inputAction.countByKey().mkString(","))

    /**
      * Actions on pair RDDs (example ({(1, 2), (2, 3), (5, 4)}))
      * collectAsMap() Collect the result as a map to provide easy lookup.
      */
    val inputCollectAsMap = sc.parallelize(List((1, 2), (2, 3), (5, 4)))
    println("Collect map:" + inputCollectAsMap.collectAsMap().mkString(","))

    /**
      * Actions on pair RDDs (example ({(1, 2), (2, 3), (3, 4)}))
      * lookup(key) Return all values associated with the provided key.
      */
    val inputLookUp = sc.parallelize(List((1, 2), (2, 3), (3, 4)))
    println("lookup key:" + inputLookUp.lookup(3).mkString(","))

    /**
      * saveAsTextFile()
      * Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem.
      */
    val inputFile = sc.textFile("src/main/resources/test_file.txt") // Load our input data.
    val count = inputFile.flatMap(line => line.split(" ")) // Split it up into words.
        .map(word => (word, 1)).reduceByKey(_ + _) // Transform into pairs and count.

    //Save the word count back out to a text file, causing evaluation.
    count.saveAsTextFile(s"src/main/resources/${UUID.randomUUID()}")
    println("OK")

    /**
      * takeSample()
      * Similar to take, in return type of array with size of n.
      * Includes boolean option of with or without replacement and random generator seed.
      */
    val takeSampleInput = sc.parallelize(List(2, 3, 4, 4))
    val takeSampleResult = takeSampleInput.takeSample(true, 1)
    println("takeSample :" + takeSampleResult.mkString(","))

  }


}





