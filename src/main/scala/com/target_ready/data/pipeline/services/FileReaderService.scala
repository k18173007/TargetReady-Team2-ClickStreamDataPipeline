package com.target_ready.data.pipeline.services

import org.apache.spark.sql.{DataFrame,SparkSession}
import com.target_ready.data.pipeline.exceptions.FileReaderException
import com.target_ready.data.pipeline.constants.ApplicationConstants.SERVER_ID

object FileReaderService {

  /** ==============================================================================================================
   *  FUNCTION TO READ DATA FROM SOURCE DIR
   *
   *  @param filePath          the location where null values will be written
   *  @param fileFormat        specifies format of the file
   *  @return                  dataframe of read data
   *  ============================================================================================================ */
  def readFile(filePath: String, fileFormat: String)(implicit spark: SparkSession): DataFrame = {

    val readFileData_df: DataFrame =
      try {
        spark
          .read
          .format(fileFormat)
          .option("header", "true")
          .load(filePath)
      }
      catch {
        case e: Exception => {
          FileReaderException("Unable to read file from the given location: " + filePath)
          spark.emptyDataFrame
        }
      }

    val readFileDataCount: Long = readFileData_df.count()
    if (readFileDataCount == 0)  throw FileReaderException("Input File is empty: " + filePath)

    readFileData_df
  }




  /** ==============================================================================================================
   *  FUNCTION TO LOAD DATA FROM KAFKA STREAM
   *
   *  @param topic    kafka topic name
   *  @return         dataframe of loaded data
   *  ============================================================================================================ */
  def loadDataFromStream(topic: String)(implicit spark: SparkSession): DataFrame = {

    val readFileData_df: DataFrame = {
      try {
        spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", SERVER_ID)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load()
      }
      catch {
        case e: Exception => {
          FileReaderException("Unable to load data from kafka topic: " + topic)
          spark.emptyDataFrame
        }
      }
    }

    readFileData_df
  }

}
