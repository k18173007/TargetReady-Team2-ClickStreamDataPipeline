package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.clenser.Clenser.{concatenateColumns, dataTypeValidation, dropDuplicates, findRemoveNullKeys, lowercaseColumns, splitColumns, trimColumn, uppercaseColumns}
import com.target_ready.data.pipeline.services.DbService.sqlReader
import com.target_ready.data.pipeline.services.FileReaderService.{loadDataFromStream, readFile}
import com.target_ready.data.pipeline.services.FileWriterService.{writeDataToSqlServer, writeDataToStream}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class PipelineServiceTest extends AnyFlatSpec with Helper {

  // Reading data from the local Input location for testing
  val testDf: DataFrame = readFile(writeTestCaseInputPath, FILE_FORMAT_TEST)(spark)
  val testDfCount: Long = testDf.count()

  /* =================================================================================================================
                                          Testing Pipeline Service
    ================================================================================================================*/

  "This Testing Method will execute whole pipeline. The test" should "fail if Input data and Staged MySql data counts doesn't match" in {

    if (testDfCount != 0) {

      //  Concatenating dataframe columns into one column and sending it to kafka stream
      val concatenatedDf: DataFrame = concatenateColumns(testDf, COLUMN_NAMES_TEST_DATA, VALUE_TEST, ",")
      writeDataToStream(concatenatedDf, TOPIC_NAME_TEST_DATA)


      //  Consuming the streaming data and applying necessary transformations for Data Quality checks
      val loadDataFromStreamDF = loadDataFromStream(TOPIC_NAME_TEST_DATA)(spark)
      val splitColumnsDf: DataFrame = splitColumns(loadDataFromStreamDF, VALUE_TEST, ",", COLUMN_NAMES_TEST_DATA)
      val changeDataTypeDF: DataFrame = dataTypeValidation(splitColumnsDf, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM)
      val uppercaseColumnsDf: DataFrame = uppercaseColumns(changeDataTypeDF)
      val trimColumnsDf: DataFrame = trimColumn(uppercaseColumnsDf)
      val RemovedNullsDf: DataFrame = findRemoveNullKeys(trimColumnsDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA, writeTestCaseOutputPath, FILE_FORMAT)
      val deDuplicatedDF: DataFrame = dropDuplicates(RemovedNullsDf, COLUMN_NAMES_TEST_DATA)
      val lowercaseColumnsDf: DataFrame = lowercaseColumns(deDuplicatedDF, COLUMNS_TO_LOWERCASE_TEST)


      //  Saving the data into staging MySql table
      writeDataToSqlServer(lowercaseColumnsDf, JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST, TIMEOUT_TEST)


      //  Reading the staged data and comparing it with Input data
      val sqlReaderDf: DataFrame = sqlReader(JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)(spark)
      val sqlReaderDfCount = sqlReaderDf.count()


      //  Comparing the Input data counts and Staged MySql data counts.
      //  Test will fail if Input data (testDfCount) and Staged MySql data (sqlReaderDfCount) have different values.
      assertResult(testDfCount)(sqlReaderDfCount)
    }
  }
}
