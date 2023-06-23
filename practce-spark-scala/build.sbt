name := "practice-spark-scala"
version := "0.1"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.spark" %% "spark-core" % "3.1.1"  % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.1.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "3.1.0"  % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.0.1",
  "org.apache.spark" %% "spark-yarn" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0",
  "org.apache.parquet" % "parquet-column" % "1.10.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
  "io.delta" %% "delta-core" % "0.8.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.271",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  "joda-time" % "joda-time" % "2.9.3",
  "org.postgresql" % "postgresql" % "42.2.22",
  "com.h2database" % "h2" % "1.4.197" % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "org.joda" % "joda-convert" % "1.8")