package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.{FileReaderException,FileWriterException}
import org.apache.spark.sql._


object DbService {

  /** ===============================================================================================================
   * FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   *
   * @param df        the dataframe taken as an input
   * @param tableName MySql table name
   * ============================================================================================================ */
  def sqlWriter(df: DataFrame, driver: String, tableName: String, jdbcUrl: String, user: String, password: String): Unit = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", driver)

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
   * @param driver        MySql driver
   * @param tableName     MySql table name
   * @param jdbcUrl       jdbc URL
   * @param user          MySql database username
   * @param password      MySql database password
   * @return              dataframe of loaded data from MySql table
   * ============================================================================================================= */
  def sqlReader(driver: String, tableName: String, jdbcUrl: String, user: String, password: String)(implicit spark: SparkSession): DataFrame = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", driver)

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
