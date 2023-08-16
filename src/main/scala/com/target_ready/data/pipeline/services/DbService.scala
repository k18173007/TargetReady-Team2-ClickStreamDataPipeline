package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.{FileReaderException,FileWriterException}
import org.apache.spark.sql._
import java.util.Properties

object DbService {

  val properties = new Properties()

  // Read properties from the configuration file
  try {
    val configFile = getClass.getResourceAsStream("/mySqlConfig.properties")
    properties.load(configFile)
  } catch {
    case e: Exception =>
      FileReaderException("Error loading configuration file: /mySqlConfig.properties")
  }

  // MySql Connection Properties
  val connectionProperties = new java.util.Properties()
  connectionProperties.put("user",properties.getProperty("spark.mysql.username"))
  connectionProperties.put("password",properties.getProperty("spark.mysql.password"))
  connectionProperties.put("driver", properties.getProperty("spark.mysql.driver"))



  /** ===============================================================================================================
   * FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   *
   * @param df        the dataframe taken as an input
   * @param tableName MySql table name
   * ============================================================================================================ */
  def sqlWriter(df: DataFrame, tableName: String, jdbcUrl: String): Unit = {

    try {
      df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, tableName, connectionProperties)
    }
    catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + jdbcUrl + " table: " + tableName)
    }

  }




  /** ===============================================================================================================
   * FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   * @param tableName     MySql table name
   * @param jdbcUrl       jdbc URL
   * @return              dataframe of loaded data from MySql table
   * ============================================================================================================= */
  def sqlReader(tableName: String, jdbcUrl: String)(implicit spark: SparkSession): DataFrame = {

    val MySqlTableData_df: DataFrame =

      try {
        spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
      }
      catch {
        case e: Exception => {
          FileReaderException("Unable to read file from: " + jdbcUrl + " table: " + tableName)
          spark.emptyDataFrame
        }
      }

    val readFileDataCount: Long = MySqlTableData_df.count()
    if (readFileDataCount == 0) throw FileReaderException("Input Table is empty: " + jdbcUrl + " table: " + tableName)

    MySqlTableData_df
  }

}
