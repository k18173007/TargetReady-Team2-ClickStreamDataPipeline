package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.services.FileReaderService._
import com.target_ready.data.pipeline.exceptions.FileReaderException
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class FileReaderServiceTest extends AnyFlatSpec with Helper{
  "readFile() mehtod" should "read data from the given location" in {
    val readFileTestDf: DataFrame = readFile(READ_LOCATION, FILE_FORMAT)
    val readFileTestCount: Long = readFileTestDf.count()
    assertResult(COUNT_SHOULD_BE)(readFileTestCount)
  }
    "readFile() method" should "throw exception in case it's not able to read data from the source" in {
      assertThrows[FileReaderException]{
        val readingFromWrongSourceDf:DataFrame=readFile(READ_WRONG_LOCATION,FILE_FORMAT)

      }

  }
}
