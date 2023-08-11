package com.target_ready.data.pipeline.util

import org.apache.spark.sql.SparkSession
import  com.target_ready.data.pipeline.constants.ApplicationConstants.{APP_NAME,MASTER_SERVER}

object ApplicationUtil {

  /** ==============================================================================================================
   *                                       FUNCTION TO CREATE SPARK SESSION
   *  ============================================================================================================ */
  def createSparkSession(): SparkSession = {
      SparkSession.builder()
        .appName(APP_NAME)
        .config("spark.sql.broadcastTimeout", "1800")
        .config("spark.sql.autoBroadcastJoinThreshold", "20485760")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .master(MASTER_SERVER)
        .getOrCreate()
  }
}
