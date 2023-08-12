package com.target_ready.data.pipeline.transform

import org.apache.spark.sql.DataFrame

object JoinTransformation {

  /** ===============================================================================================================
   *  FUNCTION TO JOIN TWO DATAFRAMES
   *
   *  @param df1          the dataframe1 taken as an input
   *  @param df2          the dataframe1 taken as an input
   *  @param joinKey      joining column name
   *  @param joinType     type of joining
   *  @return             joined dataframe
   *  ============================================================================================================ */
  def joinTable(df1: DataFrame, df2: DataFrame, joinKey: String, joinType: String): DataFrame = {

    val joinedDF = df1.join(df2, Seq(joinKey), joinType)
    joinedDF

  }
}
