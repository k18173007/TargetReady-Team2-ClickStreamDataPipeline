package com.target_ready.data.pipeline.cleanser

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.cleanser.DataCleanerMethod._
import com.target_ready.data.pipeline.services.FileReaderService._
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DataCleanerMethodTest extends AnyFlatSpec with Helper {

  import spark.implicits._


  /* =================================================================================================================
                                        Testing  Remove Duplicates Method
    ================================================================================================================*/

  "removeDuplicates() method" should "remove the duplicates from the inputDF" in {

    val deDuplicatedFileTestDf: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "LinkedIn"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "Youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val deDuplicatedDF: DataFrame = dropDuplicates(deDuplicatedFileTestDf, PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA)
    val expectedDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val checkFlag: Boolean = deDuplicatedDF.except(expectedDF).union(expectedDF.except(deDuplicatedDF)).isEmpty
    assert(checkFlag)
  }




  /* =================================================================================================================
                                           Testing  Remove Null Keys Method
    ================================================================================================================*/

  "Function  findRemoveNullKeys" should "remove nulls from the dataframe" in {

    val sampleDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "google"),
      ("30504", "11/15/2020 15:27", "android", null, "I7099", "B17543", "linkedin"),
      ("null", "11/15/2020 15:23", "android", "B000078", "NULL", "B17543", "youtube"),
      ("30385", "11/15/2020 15:24", "android", "", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    def check(df: DataFrame): Boolean = {
      val columns = df.columns
      columns.foreach { colName =>
        return df.select(col(colName)).where(col(colName).isNotNull).count() == df.count()
      }
      true
    }

    val RemovedNullsDf: DataFrame = findRemoveNullKeys(sampleDF, COLUMNS_CHECK_NULL_DQ_CHECK_TEST_DATA, writeTestCaseOutputPath, FILE_FORMAT)
    val checkFlag = check(RemovedNullsDf)

    assert(checkFlag)
  }




  /* =================================================================================================================
                                          Testing  Datatype Validation Method
    ================================================================================================================*/

  "Function  changeDataType" should "Check the data type in the dataframe " in {

    val sampleDF: DataFrame = readFile(CHANGE_DATATYPE_TEST_READ, FILE_FORMAT_TEST)
    val changeDataTypeDF: DataFrame = dataTypeValidation(sampleDF, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM)

    val result: Boolean = (changeDataTypeDF.schema("event_timestamp").dataType.typeName === "timestamp")
    assertResult(expected = true)(result)

  }




  /* =================================================================================================================
                                         Testing  Concatenate Columns Method
    ================================================================================================================*/

  "Function  ConcatenateColumns" should "Concatenate all columns into one single column " in {

    val sampleDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "LinkedIn"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "Youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val expectedDF = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE", "29839,11/15/2020 15:11,android,B000078,I7099,B17543,GOOGLE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "LinkedIn", "30504,11/15/2020 15:27,android,B000078,I7099,B17543,LinkedIn"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "Youtube", "30334,11/15/2020 15:23,android,B000078,I7099,B17543,Youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google", "30385,11/15/2020 15:24,android,B000078,I7099,D8142,google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source", "value")

    val concatenatedDf: DataFrame = concatenateColumns(sampleDF, COLUMN_NAMES_TEST_DATA, VALUE_TEST, ",")
    val checkFlag: Boolean = concatenatedDf.except(expectedDF).union(expectedDF.except(concatenatedDf)).isEmpty

    assert(checkFlag)

  }




  /* =================================================================================================================
                                           Testing  Split Columns Method
    ================================================================================================================*/

  "Function  SplitColumns" should "Split one single column into multiple columns" in {

    val sampleDF: DataFrame = Seq(
      ("29839,11/15/2020 15:11,android,B000078,I7099,B17543,GOOGLE"),
      ("30504,11/15/2020 15:27,android,B000078,I7099,B17543,LinkedIn"),
      ("30334,11/15/2020 15:23,android,B000078,I7099,B17543,Youtube"),
      ("30385,11/15/2020 15:24,android,B000078,I7099,D8142,google")
    ).toDF("value")

    val expectedDF = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "LinkedIn"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "Youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val splitColumnsDf: DataFrame = splitColumns(sampleDF, VALUE_TEST, ",", COLUMN_NAMES_TEST_DATA)
    val checkFlag: Boolean = splitColumnsDf.except(expectedDF).union(expectedDF.except(splitColumnsDf)).isEmpty

    assert(checkFlag)

  }




  /* =================================================================================================================
                                             Testing  Uppercase Columns Method
    ================================================================================================================*/

  "Function  UppercaseColumns" should "UPPERCASE the columns of dataframe" in {

    val sampleDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "google"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "linkedIn"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val expectedDF = Seq(
      ("29839", "11/15/2020 15:11", "ANDROID", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30504", "11/15/2020 15:27", "ANDROID", "B000078", "I7099", "B17543", "LINKEDIN"),
      ("30334", "11/15/2020 15:23", "ANDROID", "B000078", "I7099", "B17543", "YOUTUBE"),
      ("30385", "11/15/2020 15:24", "ANDROID", "B000078", "I7099", "D8142", "GOOGLE")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val uppercaseColumnsDf: DataFrame = uppercaseColumns(sampleDF)
    val checkFlag: Boolean = uppercaseColumnsDf.except(expectedDF).union(expectedDF.except(uppercaseColumnsDf)).isEmpty

    assert(checkFlag)

  }




  /* =================================================================================================================
                                               Testing  Lowercase Columns Method
    ================================================================================================================*/

  "Function  LowercaseColumns" should "LOWERCASE the columns of dataframe" in {
    val sampleDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "ANDROID", "B000078", "I7099", "B17543", "GOOGLE"),
      ("30504", "11/15/2020 15:27", "ANDROID", "B000078", "I7099", "B17543", "LINKEDIN"),
      ("30334", "11/15/2020 15:23", "ANDROID", "B000078", "I7099", "B17543", "YOUTUBE"),
      ("30385", "11/15/2020 15:24", "ANDROID", "B000078", "I7099", "D8142", "GOOGLE")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val expectedDF = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "google"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "linkedin"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B17543", "youtube"),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    val lowercaseColumnsDf: DataFrame = lowercaseColumns(sampleDF, COLUMNS_TO_LOWERCASE_TEST)
    val checkFlag: Boolean = lowercaseColumnsDf.except(expectedDF).union(expectedDF.except(lowercaseColumnsDf)).isEmpty

    assert(checkFlag)
  }




  /* =================================================================================================================
                                                 Testing  trim Columns Method
    ================================================================================================================*/
  "Function  trimColumn" should "Trim all columns of dataframe" in {

    val sampleDF: DataFrame = Seq(
      ("29839  ", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "google"),
      ("30504", " 11/15/2020 15:27", "android", "B000078 ", "I7099", "B17543", "linkedIn"),
      ("30334", "11/15/2020 15:23 ", "android ", "B000078", "I7099 ", "B17543", "youtube"),
      ("30385", "11/15/2020 15:24", "android", " B000078", "I7099", "D8142", "google")
    ).toDF("id", "event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")

    def check(df: DataFrame): Boolean = {
      val columns = df.columns
      columns.foreach { colName =>
        return df.select(col(colName)).where(col(colName).isNotNull).count() == df.count()
      }
      true
    }

    val trimColumnsDf: DataFrame = trimColumn(sampleDF)
    val checkFlag = check(trimColumnsDf)

    assert(checkFlag)
  }

}