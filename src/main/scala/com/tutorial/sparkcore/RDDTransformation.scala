package com.tutorial.sparkcore

import com.tutorial.utils.SparkCommon

/**
  * Created by ved on 8/1/16.
  * Transformations are operations on RDDs that return a new RDD
  */
object RDDTransformation {

  val sc = SparkCommon.sparkContext

  def main(args: Array[String]) {

    /**
      * Example of filter() Transformations  operations from external storage.
      */
    val inputRDD = sc.textFile("src/main/resources/test_file.txt")
    val dataRDD = inputRDD.filter(line => line.contains("data"))
    println(dataRDD.count())

    /**
      * Element-wise transformations
      * Example of filter()  Transformations  operations from existing collection in your program and pass it to SparkContext’s parallelize() method.
      */
    val input = sc.parallelize(List(1, 2, 3, 4,5,6,7,8))
    val result = input.filter(x => x != 1)
    println("Filter of element:" + result.collect().mkString(","))

    /**
      * Element-wise transformations
      * map() transformation  function being the new value of each element in the resulting RDD
      */
    val input1 = sc.parallelize(List(1, 2, 3, 4,5,6,7,8,9))
    val result1 = input1.map(x => x + 1)
    println("Mapping:" + result1.collect().mkString(","))

    /**
      * Basic RDD transformations in Spark
      * flatMap() to produce multiple output elements for each input element
      */
    val lines = sc.parallelize(List("hello VED", "hi"))
    val words = lines.flatMap(line => line.split(" "))
    println("flatMap:" + words.first())

    /**
      * map() transformation  function being the new value of each
      element in the resulting RDD      *
      */
    val input2 = sc.parallelize(List(1, 2, 3, 3,4,5,6))
    val result2 = input2.map(x => x + 1)
    println("map:" + result2.collect().mkString(","))

    /**
      * filter() transformation  function Return an RDD consisting of only elements
      that pass the condition passed to filter() .
      */
    val input3 = sc.parallelize(List(1, 2, 3, 4,5,6,7))
    val result3 = input3.filter(x => x != 1)
    println("Filter:" + result3.collect().mkString(","))

    /**
      * RDDs support many of the operations of mathematical sets such as distinct,union,intersection and subtract.
      * distinct() transformation  function Return an RDD consisting of Remove duplicates.
      */
    val input4 = sc.parallelize(List(1, 2, 3, 4, 2, 1,3,4,2,5,6))
    val result4 = input4.distinct()
    println("distinct:" + result4.collect().mkString(","))


    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5,7 }
      * union() transformation  function Produce an RDD containing elements from both RDDs.
      */
    val input11 = sc.parallelize(List(1, 2, 3))
    val input22 = sc.parallelize(List(3, 5, 7))
    val result1122 = input11.union(input22)
    println("Union:" + result1122.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 7}
      * intersection() transformation  function containing only elements found in both RDDs.
      */
    val input113 = sc.parallelize(List(1, 2, 3))
    val input223 = sc.parallelize(List(3, 5, 7))
    val result11223 = input113.intersection(input223)
    println("Intersection:" + result11223.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 6}
      * subtract() transformation  function Remove the contents of one RDD(e.g.,remove training data).
      */
    val inputF = sc.parallelize(List(1, 2, 3))
    val inputS = sc.parallelize(List(3, 5, 6))
    val resultSub = inputF.subtract(inputS)
    println("subtract:" + resultSub.collect().mkString(","))

    /**
      * Two-RDD transformations on RDDs containing {1, 2, 3} and {3, 5, 7}
      * cartesian() Cartesian product with the other RDD.
      */
    val inputE1 = sc.parallelize(List(1, 2, 3))
    val inputE2 = sc.parallelize(List(3, 5, 7))
    val resultE12 = inputE1.cartesian(inputE2)
    println("cartesian:" + resultE12.collect().mkString(","))

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
    val resultGroupByKey = inputTupplesList.reduceByKey((x, y) => x + y)
    println("Group values with the same key:" + resultGroupByKey.collect().mkString(","))

    /**
      * Transformations on two pair RDDs (rdd = {(1, 2), (3, 5), (3, 7)} other = {(5, 9)})
      * subtractByKey Remove elements with a key present in the other RDD.
      */

    val inputTupples = sc.parallelize(List((1, 2), (3, 5), (3, 7)))
    val inputTuppless = sc.parallelize(List((5, 9)))

    val resultSubtractByKey = inputTupples.subtractByKey(inputTuppless)
    println("Remove elements by subtractByKey:." + resultSubtractByKey.collect().mkString(","))

    /**
      * Transformations on two pair RDDs (rdd = {(1, 2), (3, 4), (4, 7)} other = {(5, 9)})
      * join Perform an inner join between two RDDs.
      */
    val inputTupple1 = sc.parallelize(List((1, 2), (3, 4), (4, 7)))
    val inputTupple2 = sc.parallelize(List((5, 9)))
    val resultJoin = inputTupple1.join(inputTupple2)
    val resultRightOuterJoin = inputTupple1.rightOuterJoin(inputTupple2)
    val resultLeftOuterJoin = inputTupple1.leftOuterJoin(inputTupple2)

    println("inner join between two RDDs." + result.collect().mkString(","))
    println(" RightOuter  join between two RDDs." + result1.collect().mkString(","))
    println(" leftOuter join between two RDDs." + result2.collect().mkString(","))

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
