package com.methods
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.TimeZone

trait SparkContext {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val AutoBroadcastJoinThresholdInBytes: Int = 128 * 1024 * 1024 // 128 MB
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  def getSparkSession(master: String = "local[*]", // spark-submit default [yarn|local]
                      appName: String = getClass.getSimpleName,
                      autoBroadcastJoinThresholdInBytes: Int = AutoBroadcastJoinThresholdInBytes,
                      broadcastTimeout: Int = 1800, // custom override
                      storageFraction: Double = 0.5, // spark default
                      logLevel: String = "ERROR"): SparkSession = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", true)
      .config("spark.sql.adaptive.enabled", true)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.autoBroadcastJoinThreshold", autoBroadcastJoinThresholdInBytes)
      .config("spark.sql.broadcastTimeout", broadcastTimeout)
      .config("spark.hadoop.fs.s3.maxRetries", "20")
      .config("spark.hadoop.fs.s3.retryPeriodSeconds", "5")
      .config("spark.hadoop.fs.s3.consistent.retryPolicyType", "exponential") // probably won't do anything -- since it's emrfs
      .config("spark.hadoop.fs.s3a.block.size", "1G")
      .config("spark.hadoop.fs.s3a.fast.upload", true)
      .config("spark.memory.storageFraction", storageFraction)
      .getOrCreate()

    //    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    //    if (conf.get("spark.master").equals("local[*]")) {
    //    }

    sparkSession.sparkContext.setLogLevel(logLevel)

    sparkSession
  }
}
