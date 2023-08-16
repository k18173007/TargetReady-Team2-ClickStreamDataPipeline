package com.target_ready.data.pipeline.util

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

object ApplicationUtil {

  // Read properties from the config file
  val config: Config = ConfigFactory.load("application.conf")

  val appName: String = config.getString("spark.app.name")
  val master: String = config.getString("spark.master")
  val executorMemory: String = config.getString("spark.executor.memory")
  val broadcastTimeout: String = config.getString("spark.sql.broadcastTimeout")
  val autoBroadcastJoinThreshold: String = config.getString("spark.sql.autoBroadcastJoinThreshold")

  /** ==============================================================================================================
   *                                      FUNCTION TO CREATE SPARK SESSION
   *  ============================================================================================================ */
  def createSparkSession(): SparkSession = {

    // Creating spark session
    val spark: SparkSession =
      SparkSession.builder()
        .appName(appName)
        .master(master)
        .config("spark.executor.memory", executorMemory)
        .config("spark.sql.broadcastTimeout", broadcastTimeout)
        .config("spark.sql.autoBroadcastJoinThreshold", autoBroadcastJoinThreshold)
        .getOrCreate()
    spark
  }

}

