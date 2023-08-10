package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.clenser.Clenser.{concatenateColumns, dataTypeValidation, dropDuplicates, findRemoveNullKeys, lowercaseColumns, splitColumns, trimColumn, uppercaseColumns}
import com.target_ready.data.pipeline.services.DbService.sqlReader
import com.target_ready.data.pipeline.services.FileReaderService.{loadDataFromStream, readFile}
import com.target_ready.data.pipeline.services.FileWriterService.{writeDataToSqlServer, writeDataToStream}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class PipelineServiceTest extends AnyFlatSpec with Helper{

  val testDf : DataFrame = readFile(writeTestCaseInputPath,fileFormat)(spark)
  val testDfCount : Long = testDf.count()

  "writeFile() method" should "write data to the given location" in {
    if (testDfCount!=0){
      val concatenatedDf:DataFrame=concatenateColumns(testDf, COLUMN_NAMES_TEST_DATA,VALUE_TEST,",")
      writeDataToStream(concatenatedDf, TOPIC_NAME_TEST_DATA)
      val loadDataFromStreamDF = loadDataFromStream(TOPIC_NAME_TEST_DATA)(spark)
      val splitColumnsDf:DataFrame=splitColumns(loadDataFromStreamDF,VALUE_TEST ,",",COLUMN_NAMES_TEST_DATA)
      val changeDataTypeDF: DataFrame = dataTypeValidation(splitColumnsDf, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM)
      val uppercaseColumnsDf:DataFrame=uppercaseColumns(changeDataTypeDF)
      val trimColumnsDf:DataFrame=trimColumn(uppercaseColumnsDf)
      val RemovedNullsDf:DataFrame=findRemoveNullKeys(trimColumnsDf,COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA,writeTestCaseOutputPath,FILE_FORMAT)
      val deDuplicatedDF: DataFrame = dropDuplicates(RemovedNullsDf, COLUMN_NAMES_TEST_DATA)
      val lowercaseColumnsDf:DataFrame=lowercaseColumns(deDuplicatedDF,COLUMNS_TO_LOWERCASE_TEST)
      writeDataToSqlServer(lowercaseColumnsDf, JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST,TIMEOUT_TEST)
      val sqlReaderDf: DataFrame = sqlReader(JDBC_DRIVER_TEST,TABLE_NAME_TEST,JDBC_URL_TEST,USER_NAME_TEST,KEY_PASSWORD_TEST)(spark)
      val checkOutputFile = sqlReaderDf.count()
      assertResult(testDfCount)(checkOutputFile)
    }
  }
}
