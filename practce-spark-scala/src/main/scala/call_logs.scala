import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import scala.collection.Seq

object call_logs extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("call-log-analysis")
    .master("local[*]")
    .getOrCreate()

  /* Read dataset call log */
  val dfCallLogs = spark.read.format("csv")
    .option("header", "true")
    .option("path", "c:\\data\\call_logs.csv")
    .load()
    .filter(col("status") === 0)

  /* Read dataset location */
  val dfLocation = spark.read.format("csv")
    .option("header", "true")
    .option("path", "c:\\data\\location_dim.csv")
    .load()

  val state_call_count = dfLocation.join(dfCallLogs, Seq("postal_code"), "inner")

  state_call_count.show(30, false)

  val aggDF = state_call_count.groupBy("state_name")
    .agg(
      count("state_name").as("call_count"),
      avg("speed").as("st_avg").cast(DecimalType(18, 2)))
  aggDF.show(30, false)

}
