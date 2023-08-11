package com.target_ready.data.pipeline.Helper


import com.target_ready.data.pipeline.constants.ApplicationConstants
import com.target_ready.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.types._

trait Helper {

  implicit val spark = createSparkSession()

  /* Helpers for File Reader Service Test Case */
  val READ_LOCATION: String = "data/Test_Inputs/FileReaderServiceTestCaseInput.csv"
  val FILE_FORMAT: String = "csv"
  val COUNT_SHOULD_BE: Int = 4
  val READ_WRONG_LOCATION: String = "data/Test_Inputs/FileReaderServiceTestCaseInp.csv"


  /* Helpers for File Writer Service Test Case */
  val writeTestCaseInputPath = "data/Test_Inputs/FileWriterServiceTestCaseInput.csv"
  val writeTestCaseOutputPath = "data/Test_Outputs/FileWriterServiceTestCaseOutput.csv"
  val FILE_FORMAT_TEST = "csv"


  /* Kafka Topic */
  val TOPIC_NAME_TEST_DATA: String = "kafkaTesting"


  /* Helper for concatenateColumns Test Case*/
  val VALUE_TEST: String = "value"
  val COLUMNS_PRIMARY_KEY_Test: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID)
  val COLUMN_NAMES_TEST_DATA: Seq[String] = Seq("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

  val TEST_SCHEMA = StructType(Seq(
    StructField("id", StringType, nullable = true),
    StructField("event_timestamp", StringType, nullable = true),
    StructField("device_type", StringType, nullable = true),
    StructField("session_id", StringType, nullable = true),
    StructField("visitor_id", StringType, nullable = true),
    StructField("item_id", StringType, nullable = true),
    StructField("redirection_source", StringType, nullable = true),
    StructField("value", StringType, nullable = false)
  ))


  /* Helpers for removeDuplicates Test Case*/
  val DEDUPLICATION_TEST_READ: String = "data/Test_Inputs/DeDuplicationTestCaseInput.csv"
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA: Seq[String] = Seq("session_id", "item_id")
  val ORDER_BY_COLUMN: String = "event_timestamp"
  val COLUMNS_TO_LOWERCASE_TEST: Seq[String] = Seq(ApplicationConstants.DEVICE_TYPE, ApplicationConstants.REDIRECTION_SOURCE)


  /* Find and Remove duplicates */
  val COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ID, ApplicationConstants.VISITOR_ID)


  /* Helpers for join Test Case*/
  val JOIN_KEY_TESTING: String = "item_id"
  val JOIN_TYPE_TESTING: String = "inner"


  /* Helpers for Change Data type test case */
  val CHANGE_DATATYPE_TEST_READ: String = "data/Test_Inputs/ChangeDataTypeTestCaseInput.csv"
  val EVENT_TIMESTAMP: String = "timestamp"
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM: Seq[String] = Seq("event_timestamp")
  val NEW_DATATYPE_CLICKSTREAM: Seq[String] = Seq("timestamp")
  val EVENT_TIMESTAMP_OPTION_TEST: String = "event_timestamp"


  /* Helpers for DQ Check Test Cases */
  val PRIMARY_KEY_COLUMNS: Seq[String] = Seq("session_id", "item_id")
  val ORDER_BY_COL: String = "event_timestamp"



  //  jdbc configurations
  val JDBC_URL_TEST: String = "jdbc:mysql://localhost:3306/stream1"
  val JDBC_DRIVER_TEST: String = "com.mysql.cj.jdbc.Driver"


  // MySql Table Names
  val TABLE_NAME_TEST = "stagingTest"
  val TABLE_NAME_FINAL_TEST = "prodTest"


  //  MySql Server Username, Password
  val USER_NAME_TEST: String = "root"
  val KEY_PASSWORD_TEST: String = "Krishna@123"


  // Timeout
  val TIMEOUT_TEST: Int = 5000

}
