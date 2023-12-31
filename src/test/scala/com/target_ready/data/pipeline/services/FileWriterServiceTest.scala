package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.Helper.Helper
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import com.target_ready.data.pipeline.services.FileReaderService._
import com.target_ready.data.pipeline.services.FileWriterService._
import com.target_ready.data.pipeline.cleanser.DataCleanerMethod._
import com.target_ready.data.pipeline.services.DatabaseService.{sqlReader, sqlWriter}


class FileWriterServiceTest extends AnyFlatSpec with Helper {

  // Reading data from the local Input location for testing
  val testDf: DataFrame = readFile(writeTestCaseInputPath, FILE_FORMAT_TEST)(spark)
  val testDfCount: Long = testDf.count()



  /* =================================================================================================================
                                          Testing File Writer Method
    ================================================================================================================*/

  "writeFile() method" should "write data to the given location" in {

    if (testDfCount != 0) {

      //  Concatenating dataframe columns into one column and sending it to kafka stream
      val concatenatedDf: DataFrame = concatenateColumns(testDf, COLUMN_NAMES_TEST_DATA, VALUE_TEST, ",")
      writeDataToStream(concatenatedDf, TOPIC_NAME_TEST_DATA)


      //  Consuming the streaming data and applying necessary transformations for Data Quality checks
      val loadDataFromStreamDF = loadDataFromStream(TOPIC_NAME_TEST_DATA)(spark)
      val splitColumnsDf: DataFrame = splitColumns(loadDataFromStreamDF, VALUE_TEST, ",", COLUMN_NAMES_TEST_DATA)
      val deDuplicatedDF: DataFrame = dropDuplicates(splitColumnsDf, COLUMN_NAMES_TEST_DATA)


      //  Saving the data into Output PostgreSQL table
      writeDataToSqlServer(deDuplicatedDF, TABLE_NAME_TEST, TIMEOUT_TEST)


      //  Reading Data from Output PostgreSQL table
      val sqlReaderDf: DataFrame = sqlReader(TABLE_NAME_TEST)(spark)
      val sqlReaderDfCount = sqlReaderDf.count()


      //  Comparing the Input data counts and Output PostgreSQL data counts.
      //  Test will fail if Input data (testDfCount) and Output PostgreSQL Table data (sqlReaderDfCount) have different values.
      assertResult(testDfCount)(sqlReaderDfCount)

    }
  }
}
