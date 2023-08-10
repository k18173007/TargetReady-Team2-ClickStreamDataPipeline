package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import com.target_ready.data.pipeline.services.FileReaderService._
import com.target_ready.data.pipeline.services.FileWriterService._
import com.target_ready.data.pipeline.clenser.Clenser._
import com.target_ready.data.pipeline.constants.ApplicationConstants.TOPIC_NAME_ITEM_DATA
import com.target_ready.data.pipeline.services.DbService.{sqlReader, sqlWriter}


class FileWriterServiceTest extends AnyFlatSpec with Helper{

  val testDf : DataFrame = readFile(writeTestCaseInputPath,fileFormat)(spark)
  val testDfCount : Long = testDf.count()

  "writeFile() method" should "write data to the given location" in {
    if (testDfCount!=0){
      val concatenatedDf:DataFrame=concatenateColumns(testDf, COLUMN_NAMES_TEST_DATA,VALUE_TEST,",")
      writeDataToStream(concatenatedDf, TOPIC_NAME_TEST_DATA)
      val loadDataFromStreamDF = loadDataFromStream(TOPIC_NAME_TEST_DATA)(spark)
      val splitColumnsDf:DataFrame=splitColumns(loadDataFromStreamDF,VALUE_TEST ,",",COLUMN_NAMES_TEST_DATA)
      val deDuplicatedDF: DataFrame = dropDuplicates(splitColumnsDf, COLUMN_NAMES_TEST_DATA)
      writeDataToSqlServer(deDuplicatedDF, JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST,TIMEOUT_TEST)
      val sqlReaderDf: DataFrame = sqlReader(JDBC_DRIVER_TEST,TABLE_NAME_TEST,JDBC_URL_TEST,USER_NAME_TEST,KEY_PASSWORD_TEST)(spark)
      val checkOutputFile = sqlReaderDf.count()
      assertResult(testDfCount)(checkOutputFile)
    }
  }
}
