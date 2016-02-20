name := """spark-tutorial"""

version := "1.0"

scalaVersion := "2.11.7"

lazy val spark = "1.6.0"


// Spark related dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-streaming" % spark,
  "org.apache.spark" %% "spark-graphx" % "1.6.0",
  "com.databricks" %% "spark-csv" % "1.3.0"

)

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"


// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

