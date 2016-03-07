package com.tutorial.sparkcore

import com.tutorial.utils.SparkCommon

/**
  * Transformations are operations on RDDs that return a new RDD
  * Created by ved on 8/1/16.
  */
object RDDTransformations {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    val inputText = sc.textFile("src/main/resources/test_file.txt")
    val inputLine = sc.parallelize(List("hello VED", "hi"))
    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8))

    /**
      * Element-wise transformations
      * map() transformation  function being the new value of each element in the resulting RDD
      */
    //val input1 = sc.parallelize(List(1, 2, 3, 4,5,6,7,8,9))
    val result1 = input.map(x => x + 1)
    println("Mapping:" + result1.collect().mkString(","))

    /**
      * Basic RDD transformations in Spark
      * flatMap() to produce multiple output elements for each input element
      */
    //val flatMapLines = sc.parallelize(List("hello VED", "hi"))
    val flatMapResult = inputLine.flatMap(line => line.split(" "))
    println("flatMap:" + flatMapResult.first())

    /**
      * Example of filter() Transformations  operations from external storage.
      */
    //val inputText = sc.textFile("src/main/resources/test_file.txt")
    val resultFilterText = inputText.filter(line => line.contains("data"))
    println("filterData" + resultFilterText.count())

    /**
      * Element-wise transformations
      * Example of filter()  Transformations  operations from existing collection in your program and
      * pass it to SparkContext’s parallelize() method.
      */

    val result = input.filter(x => x != 1)
    println("Filter of element:" + result.collect().mkString(","))

    /**
      * mapPartitions()
      * Similar to map, but runs separately on each partition (block) of the RDD,
      * so func must be of type Iterator<T> ⇒ Iterator<U> when running on an RDD of type T.
      */

    val inputData = sc.parallelize(1 to 9, 3)
    val inputData1 = sc.parallelize(1 to 9)
    val mapPartitionResult = inputData.mapPartitions(x => List(x.next).iterator)
    println("mapPartition is :" + mapPartitionResult.collect().mkString(","))

    /**
      * mapPartitionsWithIndex()
      * Similar to mapPartitions, but also provides a function
      * with an Int value to indicate the index position of the partition.
      */
    //val inputData =sc.parallelize(1 to 9 ,3)
    //val inputData1 =sc.parallelize(1 to 9 )
    val mapPartitionsWithIndexRseult = inputData.mapPartitionsWithIndex((index: Int, it: Iterator[Int])
    => it.toList.map(x => index + ", " + x).iterator)
    println("mapPartitionsWithIndex :" + mapPartitionsWithIndexRseult.collect().mkString(","))


    /**
      * sample()
      * Sample a fraction of the data, with or without replacement, using a given random number generator seed.
      */

    val sampleResult = inputData1.sample(true, .1)
    println("sample:" + sampleResult.count())

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5,7 }
      * union() transformation  function Produce an RDD containing elements from both RDDs.
      */
    val inputRdd1 = sc.parallelize(List(1, 2, 3))
    val inputRdd2 = sc.parallelize(List(3, 5, 7))
    val resultInputUnion = inputRdd1.union(inputRdd2)
    println("Union:" + resultInputUnion.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 7}
      * intersection() transformation  function containing only elements found in both RDDs.
      */
    //val input113 = sc.parallelize(List(1, 2, 3))
    ///val input223 = sc.parallelize(List(3, 5, 7))
    val resultIntersection = inputRdd1.intersection(inputRdd2)
    println("Intersection:" + resultIntersection.collect().mkString(","))

    /**
      * RDDs support many of the operations of mathematical sets such as distinct,union,intersection and subtract.
      * distinct() transformation  function Return an RDD consisting of Remove duplicates.
      */
    val distinctInput = sc.parallelize(List(1, 2, 3, 4, 2, 1, 3, 4, 2, 5, 6))
    val distinctResult = distinctInput.distinct()
    println("distinct:" + distinctResult.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 6}
      * subtract() transformation  function Remove the contents of one RDD(e.g.,remove training data).
      */
    val inputSubtract1 = sc.parallelize(List(1, 2, 3))
    val inputSubtract2 = sc.parallelize(List(3, 5, 6))
    val resultSub = inputSubtract1.subtract(inputSubtract2)
    println("subtract:" + resultSub.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 7}
      * cartesian() Cartesian product with the other RDD.
      */
    val inputCartesian1 = sc.parallelize(List(1, 2, 3))
    val inputCartesian2 = sc.parallelize(List(3, 5, 7))
    val resultCartesian = inputCartesian1.cartesian(inputCartesian2)
    println("cartesian:" + resultCartesian.collect().mkString(","))

    /**
      * Transformations on Pair RDDs.
      * Transformations on one pair RDD (example: {(1, 2), (3, 4), (4, 6)})
      * reduceByKey(func) Combine values with the same key.
      */
    val inputTupplesIs = sc.parallelize(List((1, 2), (3, 4), (4, 6)))
    val resultReduceByKey = inputTupplesIs.reduceByKey((x, y) => x + y)
    println("Combine values with the same Key:" + resultReduceByKey.collect().mkString(","))

    /**
      * Transformations on one pair RDD (example: {(1, 2), (3, 5), (3, 7)})
      * groupByKey() Group values with the same key.
      */
    val inputTupplesList = sc.parallelize(List((1, 2), (3, 5), (3, 7)))
    val resultGroupByKey = inputTupplesList.groupByKey()
    println("Group values  with the same Key:" +resultGroupByKey.collect.mkString(","))

    //println("Group values with the same key:" + resultGroupByKey.collect().mkString(","))

    /**
      * Transformations on two pair RDDs (rdd = {(1, 2), (3, 5), (3, 7)} other = {(5, 9)})
      * subtractByKey Remove elements with a key present in the other RDD.
      */

    val inputTupples = sc.parallelize(List((1, 2), (3, 5), (3, 7)))
    val inputTuppless = sc.parallelize(List((5, 9)))

    val resultSubtractByKey = inputTupples.subtractByKey(inputTuppless)
    println("Remove elements by subtractByKey:." + resultSubtractByKey.collect().mkString(","))


    /**
      * sortByKey()
      * When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys
      * in ascending or descending order, as specified in the Boolean ascending argument.
      */
    val sortKey = inputTupples.sortByKey()
    println("sortkey:" + sortKey.collect().mkString(","))

    /**
      * Transformations on two pair RDDs (rdd = {(1, 2), (3, 4), (4, 7)} other = {(5, 9)})
      * join Perform an inner join between two RDDs.
      */
    val inputTupple1 = sc.parallelize(List((1, 2), (3, 4), (4, 7)))
    val inputTupple2 = sc.parallelize(List((5, 9)))
    val resultJoin = inputTupple1.join(inputTupple2)
    val resultRightOuterJoin = inputTupple1.rightOuterJoin(inputTupple2)
    val resultLeftOuterJoin = inputTupple1.leftOuterJoin(inputTupple2)

    println("inner join between two RDDs." + resultJoin.collect().mkString(","))
    println(" RightOuter  join between two RDDs." + resultRightOuterJoin.collect().mkString(","))
    println(" leftOuter join between two RDDs." + resultLeftOuterJoin.collect().mkString(","))

    /**
      * Transformations on two pair RDDs (rdd = {(1, 2), (3, 4), (4, 6)} other = {(4, 9)})
      * cogroup Group data from both RDDs sharing the same key.
      */
    val inputElementTupple1 = sc.parallelize(List((1, 2), (3, 4), (4, 6)))
    val inpuElementtTupple2 = sc.parallelize(List((4, 9)))

    val resultGroup = inputElementTupple1.cogroup(inputElementTupple1)
    println("Group data from both RDDs sharing the same key :" + resultGroup.collect().mkString(","))


  }

}
