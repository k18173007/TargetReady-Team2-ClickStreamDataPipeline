package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import org.scalatest.flatspec.AnyFlatSpec
import com.target_ready.data.pipeline.dqCheck.DataQualityCheckMethods.{DqDuplicateCheck, dqNullCheck}
import com.target_ready.data.pipeline.services.DatabaseService.{sqlReader, sqlWriter}
import org.apache.spark.sql.DataFrame


class DataQualityCheckServiceTest extends AnyFlatSpec with Helper {

  //  Creating Sample Dataframes for Testing
  val testDf: DataFrame = sqlReader(TABLE_NAME_TEST)(spark)
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
    sqlWriter(testDf, TABLE_NAME_FINAL_TEST)


    //  Executing sqlReader() Method
    val sqlReaderDf: DataFrame = sqlReader(TABLE_NAME_FINAL_TEST)(spark)


    //  Comparing Expected Dataframe and Dq check Dataframe
    val checkFlag: Boolean = sqlReaderDf.except(expectedDF).union(expectedDF.except(sqlReaderDf)).isEmpty


    //  Test will fail if any of the ( DqCheckNullFlag, DqDuplicateCheckFlag, checkFlag) flags returns false.
    assert(DqCheckNullFlag && DqDuplicateCheckFlag && checkFlag, "Conditions not satisfied")

  }
}
