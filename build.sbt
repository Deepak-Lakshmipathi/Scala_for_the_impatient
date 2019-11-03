name := "Scala for the impatient"

version := "0.1"

scalaVersion := "2.12.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided"

