package com.target_ready.data.pipeline.util

import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.target_ready.data.pipeline.exceptions.FileReaderException

object ApplicationUtil {

  /** ==============================================================================================================
   *                                        FUNCTION TO CREATE SPARK SESSION
   *  ============================================================================================================ */
  def createSparkSession(): SparkSession = {

    val properties = new Properties()

    // Read properties from the configuration file
    try {
      val configFile = getClass.getResourceAsStream("/sparkConfig.properties")
      properties.load(configFile)
    } catch {
      case e: Exception =>
        FileReaderException("Error loading configuration file: /sparkConfig.properties")
    }


    // Creating spark session
    val spark: SparkSession =
      SparkSession.builder()
        .appName(properties.getProperty("spark.app.name"))
        .master(properties.getProperty("spark.master"))
        .config("spark.executor.memory", properties.getProperty("spark.executor.memory"))
        .config("spark.sql.broadcastTimeout", properties.getProperty("spark.sql.broadcastTimeout"))
        .config("spark.sql.autoBroadcastJoinThreshold", properties.getProperty("spark.sql.autoBroadcastJoinThreshold"))
        .getOrCreate()
    spark
  }

}

