package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import org.scalatest.flatspec.AnyFlatSpec
import com.target_ready.data.pipeline.dqCheck.DqCheckMethods.{DqDuplicateCheck, dqNullCheck}
import com.target_ready.data.pipeline.services.DbService.{sqlReader, sqlWriter}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
class DqCheckServiceTest extends AnyFlatSpec with Helper{

  val testDf: DataFrame = sqlReader(JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)(spark)
  val testDfCount: Long = testDf.count()

  "Method executeDqCheck()" should "throw an exception (FileReaderException, FileWriterException, " +
    "DqNullCheckException, DqDupCheckException) in-case if there is any" in {

    val expectedDF:DataFrame=testDf
    val DqCheckNullFlag: Boolean = dqNullCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA)
    val DqDuplicateCheckFlag: Boolean = DqDuplicateCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA,EVENT_TIMESTAMP_OPTION_TEST)
    sqlWriter(testDf, JDBC_DRIVER_TEST, TABLE_NAME_FINAL_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)
    val sqlReaderDf: DataFrame = sqlReader(JDBC_DRIVER_TEST, TABLE_NAME_FINAL_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)(spark)
    val checkFlag: Boolean = sqlReaderDf.except(expectedDF).union(expectedDF.except(sqlReaderDf)).isEmpty

    assert(DqCheckNullFlag && DqDuplicateCheckFlag && checkFlag, "Conditions not satisfied")

  }
  }
