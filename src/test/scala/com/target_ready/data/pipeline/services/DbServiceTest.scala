package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.services.DbService._
import com.target_ready.data.pipeline.services.FileReaderService.readFile
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DbServiceTest extends AnyFlatSpec with Helper {

  //  Creating Sample Dataframes for Testing
  val testDf: DataFrame = readFile(writeTestCaseInputPath, FILE_FORMAT_TEST)(spark)
  val testDfCount: Long = testDf.count()



  /* =================================================================================================================
                                        Testing  Write Data to MySql Database Method
    ================================================================================================================*/

  "Function sqlReader" should "write the data into the output MySql Table" in {

    sqlWriter(testDf, TABLE_NAME_TEST)
    val sqlReaderDf: DataFrame = sqlReader(TABLE_NAME_TEST)(spark)

    val sqlReaderDfCount = sqlReaderDf.count()
    if (testDfCount != 0) assertResult(testDfCount)(sqlReaderDfCount)

  }



  /* =================================================================================================================
                                          Testing  Reading Data from MySql Database Method
    ================================================================================================================*/

  "Function sqlReader" should "read the data from the source MySql Table" in {

    val expectedDF = testDf
    val sqlReaderDf: DataFrame = sqlReader(TABLE_NAME_TEST)(spark)

    val checkFlag: Boolean = sqlReaderDf.except(expectedDF).union(expectedDF.except(sqlReaderDf)).isEmpty
    assert(checkFlag)
  }
}
