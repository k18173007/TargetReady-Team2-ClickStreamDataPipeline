package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import org.scalatest.flatspec.AnyFlatSpec
import com.target_ready.data.pipeline.dqCheck.DqCheckMethods.{DqDuplicateCheck, dqNullCheck}
import com.target_ready.data.pipeline.services.DbService.{sqlReader, sqlWriter}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DqCheckServiceTest extends AnyFlatSpec with Helper {

  //  Creating Sample Dataframes for Testing
  val testDf: DataFrame = sqlReader(JDBC_DRIVER_TEST, TABLE_NAME_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)(spark)
  val testDfCount: Long = testDf.count()



  /* =================================================================================================================
                                          Testing Dq check Service
    ================================================================================================================*/
  "Method executeDqCheck()" should "throw an exception (FileReaderException, FileWriterException, " +
    "DqNullCheckException, DqDupCheckException) in-case if there is any" in {

    //  Expected Dataframe
    val expectedDF: DataFrame = testDf


    //  Executing dqNullCheck() and DqDuplicateCheck() Methods
    val DqCheckNullFlag: Boolean = dqNullCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA)
    val DqDuplicateCheckFlag: Boolean = DqDuplicateCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA, EVENT_TIMESTAMP_OPTION_TEST)


    //  Executing sqlWriter() Method
    sqlWriter(testDf, JDBC_DRIVER_TEST, TABLE_NAME_FINAL_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)


    //  Executing sqlReader() Method
    val sqlReaderDf: DataFrame = sqlReader(JDBC_DRIVER_TEST, TABLE_NAME_FINAL_TEST, JDBC_URL_TEST, USER_NAME_TEST, KEY_PASSWORD_TEST)(spark)


    //  Comparing Expected Dataframe and Dq check Dataframe
    val checkFlag: Boolean = sqlReaderDf.except(expectedDF).union(expectedDF.except(sqlReaderDf)).isEmpty


    //  Test will fail if any of the ( DqCheckNullFlag, DqDuplicateCheckFlag, checkFlag) flags returns false.
    assert(DqCheckNullFlag && DqDuplicateCheckFlag && checkFlag, "Conditions not satisfied")

  }
}
