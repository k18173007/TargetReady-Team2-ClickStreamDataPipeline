package com.target_ready.data.pipeline.constants

object ApplicationConstants {

  /** ===============================================================================================================
   *                                                SERVER CONFIG PROPS
   *  ============================================================================================================== */

  //  Host server id
  val SERVER_ID: String = "localhost:9092"

  //  Kafka Topic Names
  val TOPIC_NAME_ITEM_DATA: String = "itemData"
  val TOPIC_NAME_CLICKSTREAM_DATA: String = "clickStreamData"

  val CHECKPOINT_LOCATION: String = "checkpoint"


  /** ===============================================================================================================
   *                                              INPUT-OUTPUT formats, paths
   *  ============================================================================================================== */

  //  ITEM_DATA
  val INPUT_FORMAT_ITEM_DATA: String = "csv"
  val OUTPUT_FORMAT_ITEM_DATA: String = "orc"
  val INPUT_FILE_PATH_ITEM_DATA: String = "data/Input/item/item_data.csv"

  //  ClickStream Data
  val INPUT_FORMAT_CLICKSTREAM: String = "csv"
  val OUTPUT_FORMAT__CLICKSTREAM: String = "orc"
  val INPUT_FILE_PATH_CLICKSTREAM_DATA: String = "data/Input/clickstream/clickstream_log.csv"

  //  Output file path
  val OUTPUT_FILE_PATH: String = "data/output"
  val NULL_VALUE_PATH: String = "data/output/null_value_output/null_outputs.orc"

  //  Null value file path
  val NULL_VALUE_FILE_FORMAT: String = "orc"


  /** ===============================================================================================================
   *                                              INPUT DATA COLUMN NAMES
   *  ============================================================================================================== */

  //  Item Data
  val DUP_VALUE_CHECK_COLUMN: String = "item_id"
  val ITEM_ID: String = "item_id"
  val ITEM_PRICE: String = "item_price"
  val PRODUCT_TYPE: String = "product_type"
  val DEPARTMENT_NAME: String = "department_name"
  val COLUMNS_PRIMARY_KEY_ITEM_DATA: Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  val COLUMN_NAMES_ITEM_DATA: List[String] = List(ITEM_ID, ITEM_PRICE, PRODUCT_TYPE, DEPARTMENT_NAME)
  val COLUMNS_CHECK_NULL_DQ_CHECK_ITEM_DATA: Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  val COLUMNS_TO_LOWERCASE_ITEM_DATA: Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)


  //  ClickStream Data
  val ID: String = "id"
  val EVENT_TIMESTAMP: String = "event_timestamp"
  val DEVICE_TYPE: String = "device_type"
  val SESSION_ID: String = "session_id"
  val VISITOR_ID: String = "visitor_id"
  val REDIRECTION_SOURCE: String = "redirection_source"
  val VALUE: String = "value"
  val COLUMNS_PRIMARY_KEY_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID)
  val COLUMN_NAMES_CLICKSTREAM_DATA: Seq[String] = Seq(ApplicationConstants.ID, ApplicationConstants.EVENT_TIMESTAMP, ApplicationConstants.DEVICE_TYPE, ApplicationConstants.SESSION_ID, ApplicationConstants.VISITOR_ID, ApplicationConstants.ITEM_ID, ApplicationConstants.REDIRECTION_SOURCE)
  val COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ID, ApplicationConstants.VISITOR_ID)
  val EVENT_TIMESTAMP_OPTION: String = "event_timestamp"
  val COLUMNS_TO_LOWERCASE_CLICKSTREAM_DATA: Seq[String] = Seq(ApplicationConstants.DEVICE_TYPE, ApplicationConstants.REDIRECTION_SOURCE)


  //  timestamp datatype and timestamp format for changing datatype
  val TIMESTAMP_DATATYPE: String = "timestamp"
  val TIMESTAMP_FORMAT: String = "MM/dd/yyyy H:mm"

  //  column for Changing DATATYPE
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val COLUMNS_VALID_DATATYPE_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)

  //  new DATATYPE
  val NEW_DATATYPE_CLICKSTREAM: Seq[String] = Seq("timestamp")
  val NEW_DATATYPE_ITEM: Seq[String] = Seq("float")

  //  join
  val JOIN_KEY: String = "item_id"
  val JOIN_TYPE_NAME: String = "inner"

  //  Row Number
  val ROW_NUMBER: String = "row_number"
  val ROW_CONDITION: String = "row_number == 1"


  /** ============================================================================================================
   *                                            PostgreSQL SERVER CONFIGURATIONS
   * ============================================================================================================ */

  // PostgreSQL Table Names
  val STAGING_TABLE = "staging"
  val PROD_TABLE = "prod"

  // Timeout
  val SAVE_DATA_TO_SQL_TABLE_TIMEOUT: Int = 150000
  val SAVE_DATA_TO_LOCAL_DIR_TIMEOUT: Int = 10000


  /** ============================================================================================================
   *                                              PIPELINE EXIT CODES
   * ============================================================================================================ */

  val FAILURE_EXIT_CODE: Int = 0
  val SUCCESS_EXIT_CODE: Int = 1

}
