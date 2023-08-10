package com.target_ready.data.pipeline.dqCheck

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.constants.ApplicationConstants.COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA
import com.target_ready.data.pipeline.dqCheck.DqCheckMethods.{dqNullCheck,DqDuplicateCheck}
import com.target_ready.data.pipeline.services.DbService.sqlReader
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DqCheckMethodsTest extends AnyFlatSpec with Helper{
  val testDf: DataFrame = sqlReader(JDBC_DRIVER_TEST,TABLE_NAME_TEST,JDBC_URL_TEST,USER_NAME_TEST,KEY_PASSWORD_TEST)(spark)
  val testDfCount: Long = testDf.count()

  "Method dqNullCheck()" should "throw an exception if there is any nulls present in Df" in{
    if (testDfCount!=0){
      val dfCheckNullFlag: Boolean = dqNullCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA)
      assert(dfCheckNullFlag)

    }
  }

  "Method DqDuplicateCheck()" should "throw an exception if there are any duplicates present in Df" in{
    if (testDfCount!=0){
      val DqDuplicateCheckFlag: Boolean = DqDuplicateCheck(testDf, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA,EVENT_TIMESTAMP_OPTION_TEST)
      assert(DqDuplicateCheckFlag)

    }
  }
}
