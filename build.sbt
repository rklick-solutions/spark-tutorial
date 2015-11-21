name := """spark-tutorial"""

version := "1.0"

scalaVersion := "2.11.7"

// Spark related dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

