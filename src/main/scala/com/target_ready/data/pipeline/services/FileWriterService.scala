package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.{FileWriterException,FileReaderException}
import com.target_ready.data.pipeline.constants.ApplicationConstants.{CHECKPOINT_LOCATION, SERVER_ID}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Properties

object FileWriterService {

  /** ===============================================================================================================
   *  FUNCTION TO WRITE DATA INTO KAFKA STREAM
   *
   *  @param df             the dataframe taken as an input
   *  @param topic          kafka topic name
   *  ==============================================================================================================*/
  def writeDataToStream(df: DataFrame, topic: String): Unit = {
    try {
      df
        .selectExpr("CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", SERVER_ID)
        .option("topic", topic)
        .save()
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + topic)
    }
  }




  /** ===============================================================================================================
   *  FUNCTIONS TO SAVE DATA INTO OUTPUT LOCATION
   *
   *  @param df             the dataframe taken as an input
   *  @param filePath       the location where null values will be written
   *  @param fileFormat     specifies format of the file
   *  ==============================================================================================================*/
  def writeDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String,timeout:Int): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
        .awaitTermination(timeoutMs = timeout)

    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }




  /** ===============================================================================================================
   *  FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   *  @param df          the dataframe taken as an input
   *  @param tableName   MySql table name
   *  @param jdbcUrl     jdbc URL
   *  ============================================================================================================ */
  def writeDataToSqlServer(df: DataFrame, tableName: String, jdbcUrl: String, timeout: Int): Unit = {

    val properties = new Properties()

    // Read properties from the configuration file
    try {
      val configFile = getClass.getResourceAsStream("/mySqlConfig.properties")
      properties.load(configFile)
    } catch {
      case e: Exception =>
        FileReaderException("Error loading configuration file: /mySqlConfig.properties")
    }

    val userName :String = properties.getProperty("spark.mysql.username")
    val password :String = properties.getProperty("spark.mysql.password")
    val driver :String = properties.getProperty("spark.mysql.driver")

    try {
      df.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("jdbc")
            .option("driver", driver)
            .option("url", jdbcUrl)
            .option("dbtable", tableName)
            .option("user", userName)
            .option("password", password)
            .mode("overwrite")
            .save()
        }
        .outputMode(OutputMode.Append())
        .start().awaitTermination(timeoutMs = timeout)
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + jdbcUrl + "/" + tableName)
    }
  }




  /** ===============================================================================================================
   *  FUNCTION TO SAVE NULL-VALUE DATA INTO NULL-VALUE-OUTPUT LOCATION
   *
   *  @param df             the dataframe taken as an input
   *  @param filePath       the location where null values will be written
   *  @param fileFormat     specifies format of the file
   *  ============================================================================================================== */
  def writeNullDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String,timeout:Int): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(Trigger.Once())
        .start()
        .awaitTermination(timeoutMs = timeout)
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }

}
