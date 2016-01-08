package com.tutorial.sparkcore

import com.tutorial.utils.SparkCommon

/**
  * Created by ved on 8/1/16.
  * Actions are operations that return a result to the driver program or write it to storage
  */
object RDDAction {

  val sc = SparkCommon.sc

  def main(args: Array[String])
  {
    /**
      * The most common action on basic RDDs
      * reduce(func) Combine the elements of the RDD together in parallel.
      */
    val x = sc.parallelize(List(3, 2, 4, 6))
    val y = sc.parallelize(List(2, 4, 2, 3))

    val rUnion = x.union(y)
    val resultReduce = rUnion.reduce((x, y) => x + y)
    println("reduce:" + resultReduce + " ")

    /**
      * The most common action on basic RDDs
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * collect() Return all elements from the RDD.
      */
    val inputElement = sc.parallelize(List(2, 3, 4, 4))
    println(inputElement.collect().mkString(","))

    /**
      * Basic actions on an RDD containing {2, 3, 4, 4}
      * count() returns a count of the elements the RDD..
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
    val inputBy = sc.parallelize(List(2, 3, 4, 4))
    println("Take Order :" + inputBy.takeOrdered(2).mkString(","))

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
    println("Collect the result as a map to provide easy lookup:" + inputCollectAsMap.collectAsMap().mkString(","))

    /**
      * Actions on pair RDDs (example ({(1, 2), (2, 3), (3, 4)}))
      * lookup(key) Return all values associated with the provided key.
      */
    val inputLookUp = sc.parallelize(List((1, 2), (2, 3), (3, 4)))
    println("Return all values associated with the provided key:" + inputLookUp.lookup(3).mkString(","))


  }


}





