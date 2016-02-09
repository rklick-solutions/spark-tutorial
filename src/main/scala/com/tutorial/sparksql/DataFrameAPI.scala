package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame

/**
  * Created by ved on 9/2/16.
  * DataFarme API Example Using Different types of Functionality.
  * Diiferent type of DataFrame operatios are :
  */

/**
  * Action:
  * Action are operations (such as take, count, first, and so on) that return a value after
  * running a computation on an DataFrame.
  */
object DataFrameAPI {

  val sc = SparkCommon.sparkContext

  val sqlContext = SparkCommon.sparkSQLContext
  /**
    * Use the following command to create SQLContext.
    */
  val ssc = SparkCommon.sparkSQLContext

  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]) {

    val employee = "src/main/resources/employee.json"

    val employees = "src/main/resources/employees.json"

    val empDataFrame: DataFrame = ssc.read.format("json").options(schemaOptions).load(employee)

    val empDataFrame1: DataFrame = ssc.read.format("json").options(schemaOptions).load(employees)

    /**
      * Some Action Operation with examples:
      * show()
      * If you want to see top 20 rows of DataFrame in a tabular form then use the following command.
      */

    empDataFrame.show()


    /**
      * show(n)
      * If you want to see n rows of DataFrame in a tabular form then use the following command.
      */

    empDataFrame.show(2)


    /**
      * take()
      * take(n) Returns the first n rows in the DataFrame.
      */
    empDataFrame.take(2).foreach(println)


    /**
      * count()
      * Returns the number of rows.
      */

    empDataFrame.groupBy("age").count().show()


    /**
      * head()
      * head () is used to returns first row.
      */

    val resultHead = empDataFrame.head()

    println(resultHead.mkString(","))

    /**
      * head(n)
      * head(n) returns first n rows.
      */

    val resultHeadNo = empDataFrame.head(3)

    println(resultHeadNo.mkString(","))

    /**
      * first()
      * Returns the first row.
      */

    val resultFirst = empDataFrame.first()

    println("fist:" + resultFirst.mkString(","))


    /**
      * collect()
      * Returns an array that contains all of Rows in this DataFrame.
      */

    val resultCollect = empDataFrame.collect()

    println(resultCollect.mkString(","))


    /**
      * Basic DataFrame functions:
      *
      */
    /**
      * printSchema()
      * If you want to see the Structure (Schema) of the DataFrame, then use the following command.
      */

    empDataFrame.printSchema()


    /**
      * toDF()
      * toDF() Returns a new DataFrame with columns renamed.
      * It can be quite convenient in conversion from a RDD of tuples into a DataFrame with meaningful names.
      */

    val empl = sc.textFile("src/main/resources/employee.txt")
      .map(_.split(","))
      .map(emp => Employee(emp(0).trim.toInt, emp(1), emp(2).trim.toInt))
      .toDF().show()


    /**
      * dtypes()
      * Returns all column names and their data types as an array.
      */

    empDataFrame.dtypes.foreach(println)


    /**
      * columns ()
      * Returns all column names as an array.
      */

    empDataFrame.columns.foreach(println)

    /**
      * cache()
      * cache() explicitly to store the data into memory.
      * Or data stored in a distributed way in the memory by default.
      */

    val resultCache = empDataFrame.filter(empDataFrame("age") > 23)

    resultCache.cache().show()


    /**
      * persist()
      * persist() explicitly to store the data into memory.
      * Or data stored in a distributed way in the memory by default.
      */

   // val resultCache1 = empDataFrame.filter(empDataFrame("age") > 23)

    //resultCache1.persist().show()

    /**
      * unpersist()
      * unpersist() remove all blocks for it from memory and disk.
      */
    //resultCache1.unpersist().show()

    /**
      * Data Frame operations:
      */
    /**
      * orderBy()
      * Returns a new DataFrame sorted by the specified column(s).
      */
    empDataFrame.orderBy(desc("age")).show()


    /**
      * groupBy()
      * counting the number of employees who are of the same age.
      */

    empDataFrame.groupBy("age").count().show()


    /**
      * na()
      * Returns a DataFrameNaFunctions for working with missing data.
      */


    empDataFrame.na.drop().show()

    /**
      * stat()
      * Returns a DataFrameStatFunctions for working statistic functions support.
      */
    //empDataFrame.stat.freqItems(Seq("a")).show()


    /**
      * join()
      * Cartesian join with another DataFrame.
      */

    // val resultJoin = empDataFrame.join(empDataFrame, empDataFrame1("userId") === empDataFrame("userId"))
    //resultJoin.show()

    //join(department, people("deptId") === department("id"))

    /**
      * sort()
      * Returns a new DataFrame sorted by the given expressions.
      */
    empDataFrame.sort($"userId", $"name".desc).show()


    /**
      * apply()
      * Selects column based on the column name and return it as a Column.
      * Note that the column name can also reference to a nested column like a.b.
      */

    //empDataFrame.apply("name")

    /**
      * col()
      * Selects column based on the column name and return it as a Column
      */

    // val resultCol = empDataFrame.col("age" +10)
    // println(resultCol)

    /**
      * as()
      * Returns a new DataFrame with an alias set.
      */

    empDataFrame.select(avg($"age").as("average_age")).show()


    /**
      * alias()
      * Returns a new DataFrame with an alias set. Same as `as`.
      */

    empDataFrame.select(avg($"age").alias("average_age")).show()


    /**
      * select()
      * to fetch age-column among three columns from the DataFrame.
      */

    empDataFrame.select("age").show()


    /**
      * filter()
      * filter the employees whose age is less than 28 (age < 28).
      */
    empDataFrame.filter(empDataFrame("age") > 23).show()


    /**
      * where()
      * Filters age using the given SQL expression.
      */
    empDataFrame.where($"age" > 25).show()


    /**
      * rollup()
      * Create a multi-dimensional rollup for
      * the current DataFrame using the specified columns.
      */

    empDataFrame1.rollup($"firstName", $"jobTitleName").avg()

    /**
      * cube()
      * Create a multi-dimensional cube for the current DataFrame using the specified columns,
      * so we can run aggregation on them.
      */

    empDataFrame1.cube($"employeeCode", $"jobTitleName").agg(Map(
      "salary" -> "avg",
      "age" -> "max")).show()


    /**
      * agg()
      * Aggregates on the entire DataFrame without groups.
      */

    empDataFrame.agg(max($"age")).show()


    /**
      * limit()
      * Returns a new DataFrame by taking the first n rows.
      * The difference between this function and head is that head returns an array
      * while limit returns a new DataFrame.
      */

    empDataFrame1.limit(3).show()


    /**
      * unionAll()
      * Returns a new DataFrame containing union of rows in this frame and another frame.
      */
    //empDataFrame.unionAll(empDataFrame1).show()


    /**
      * intersect()
      * Returns a new DataFrame containing rows only in both this frame and another frame.
      */

    //empDataFrame1.intersect(empDataFrame).show()


    /**
      * except()
      * Returns a new DataFrame containing rows in this frame but not in another frame.
      */
    //empDataFrame.except(empDataFrame1).show()


    /**
      * sample()
      * Returns a new DataFrame by sampling a fraction of rows.
      */


    /**
      * randomSplit()
      * Randomly splits this DataFrame with the provided weights.
      */


    /**
      * explode()
      * Returns a new DataFrame where each row has been expanded to zero or more rows by the provided function.
      */

    //empDataFrame1.explode("words", "word"){words: String => words.split(" ")}.show()


    /**
      * withColumn()
      * Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
      */
    //empDataFrame.withColumn("$salary","$salry").show()

    //empDataFrame.withColumn("young", > 25)


    /**
      * withColumnRenamed()
      * Returns a new DataFrame with a column renamed.
      *
      */

    empDataFrame1.withColumnRenamed("employeeCode", "employeeId").show()


    /**
      * drop()
      * Returns a new DataFrame with a column dropped.
      */

    empDataFrame.drop("name").show()


    /**
      * dropDuplicates()
      * Returns a new DataFrame that contains only the unique rows from this DataFrame.
      * This is an alias for distinct.
      */
    empDataFrame.dropDuplicates().show()


    /**
      * describe()
      * escribe returns a DataFrame containing information such as number of non-null entries (count),
      * mean, standard deviation, and minimum and maximum value for each numerical column.
      */
    empDataFrame.describe("age").show()

  }


}

case class Employee(id: Int, name: String, age: Int)
