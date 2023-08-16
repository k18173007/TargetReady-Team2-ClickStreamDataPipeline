package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.{FileReaderException, FileWriterException}
import org.apache.spark.sql._
import com.typesafe.config.{Config, ConfigFactory}

object DbService {

  // Read properties from the config file
  val config: Config = ConfigFactory.load("application.conf")

  val user: String = config.getString("database.user")
  val password: String = config.getString("database.password")
  val jdbcUrl: String = config.getString("database.url")
  val driver: String = config.getString("database.driver")

  val connectionProperties = new java.util.Properties()
  connectionProperties.put("user", user)
  connectionProperties.put("password", password)
  connectionProperties.put("driver", driver)

  /** ===============================================================================================================
   * FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   *
   * @param df        the dataframe taken as an input
   * @param tableName MySql table name
   *                  ============================================================================================================ */
  def sqlWriter(df: DataFrame, tableName: String): Unit = {

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
   *
   * @param tableName MySql table name
   * @return dataframe of loaded data from MySql table
   *         ============================================================================================================= */
  def sqlReader(tableName: String)(implicit spark: SparkSession): DataFrame = {

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
