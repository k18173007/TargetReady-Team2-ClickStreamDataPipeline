package com.target_ready.data.pipeline.transform

import com.target_ready.data.pipeline.Helper.Helper
import com.target_ready.data.pipeline.services.DbService.sqlWriter
import com.target_ready.data.pipeline.transform.JoinTransformation._
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{DataFrame}

class JoinTransformationTest extends AnyFlatSpec with Helper{

  /* =================================================================================================================
                                          Testing  Join Dataframes Method
     ================================================================================================================*/

  "join() method" should "perform inner join of two dataframes" in {
    import spark.implicits._
    val clickstreamDf : DataFrame = Seq(
      ("29839","11-15-2020 15:11:00","android","B000078","I7099","B17543","GOOGLE"),
      ("30504","11-15-2020 15:27:00","android","B000078","I7099","B19304","LinkedIn"),
      ("30334","11-15-2020 15:23:00","android","B000078","I7099","B29093","Youtube")
    ).toDF("id","event_timestamp","device_type","session_id","visitor_id","item_id","redirection_source")
    val itemDf : DataFrame = Seq(
      ("B17543","6784","D634","Garden & Outdoors"),
      ("B19304","1320.5","C159","Baby"),
      ("B29093","409.5","H872","Furniture"),
      ("F4939","1756.5","G822","Collectibles"),
      ("E0383","412.5","B619","Apps & Games")
    ).toDF("item_id","item_price","product_type","department_name")

    val jointDf: DataFrame = joinTable(clickstreamDf, itemDf,JOIN_KEY_TESTING,JOIN_TYPE_TESTING)
    sqlWriter(jointDf, JOIN_TABLE_NAME_TEST)

    val expectedDf : DataFrame = Seq(
      ("B17543","29839","11-15-2020 15:11:00","android","B000078","I7099","GOOGLE","6784","D634","Garden & Outdoors"),
      ("B19304","30504","11-15-2020 15:27:00","android","B000078","I7099","LinkedIn","1320.5","C159","Baby"),
      ("B29093","30334","11-15-2020 15:23:00","android","B000078","I7099","Youtube","409.5","H872","Furniture")
    ) .toDF("item_id","id","event_timestamp","device_type","session_id","visitor_id","redirection_source","item_price","product_type","department_name")

    val checkFlag:Boolean = jointDf.except(expectedDf).union(expectedDf.except(jointDf)).isEmpty
    assert(checkFlag)
   }
}
