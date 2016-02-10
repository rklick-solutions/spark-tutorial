package com.tutorial.sparksql

import com.tutorial.utils.SparkCommon
import org.apache.spark.sql.DataFrame

import scala.reflect.internal.util.TableDef.Column

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

    val employee1 = "src/main/resources/employee1.json"

    val employees = "src/main/resources/employees.json"

    val empDataFrame: DataFrame = ssc.read.format("json").options(schemaOptions).load(employee)

    val empDataFrame2: DataFrame = ssc.read.format("json").options(schemaOptions).load(employee1)

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
      * sort()
      * Returns a new DataFrame sorted by the given expressions.
      */
    empDataFrame.sort($"userId".desc).show()


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
      * to fetch age-column among all columns from the DataFrame.
      */

    empDataFrame.select("age").show()


    /**
      * filter()
      * filter the employees whose age is greater than 28 (age > 28).
      */

    empDataFrame.filter(empDataFrame("age") > 28).show()


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
      * returns the average of the values in a group.
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

    empDataFrame.unionAll(empDataFrame2).show()


    /**
      * intersect()
      * Returns a new DataFrame containing rows only in both this frame and another frame.
      */

    empDataFrame2.intersect(empDataFrame).show()


    /**
      * except()
      * Returns a new DataFrame containing rows in this frame but not in another frame.
      */
    empDataFrame.except(empDataFrame2).show()




    /**
      * withColumn()
      * Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
      */

    val coder: (Int => String) = (arg: Int) => {if (arg < 28) "little" else "big"}

    val sqlfunc = udf(coder)

    empDataFrame.withColumn("First",sqlfunc(col("age"))).show()


    /**
      * withColumnRenamed()
      * Returns a new DataFrame with a column renamed.
      *
      */

    empDataFrame2.withColumnRenamed("id", "employeeId").show()


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
      * describe returns a DataFrame containing information such as number of non-null entries (count),
      * mean, standard deviation, and minimum and maximum value for each numerical column.
      */
    empDataFrame.describe("age").show()

  }


}

case class Employee(id: Int, name: String, age: Int)
