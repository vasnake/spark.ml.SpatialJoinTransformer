package me.valik.spark.transformer

import org.scalatest._
import org.apache.spark.sql.{DataFrame, Row}
import me.valik.spark.test._

class BroadcastSpatialJoinTestWithCustomSpark extends
  FlatSpec with Matchers with SimpleLocalSpark with DataFrameTestTools {

  import BroadcastSpatialJoinTest._

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTestWithCustomSpark -- -z "smoke"
  it should "pass smoke test" in {
    import spark.implicits._

    val input = parsePointID(
      """
        |i1, 1, 1
        |i2, 2, 2
      """.stripMargin).toDS

    val data = parsePoiID(
      """
        |d1, 1.1, 1.1
        |d2, 2.1, 2.1
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
      """.stripMargin).toDS

    val res = makeTransformer(data.toDF).transform(input)
    res.show(3, truncate=false)

    //res.collect should contain theSameElementsAs Seq(
    //  Row("i1", 1d, 1d, "d1"),
    //  Row("i2", 2d, 2d, "d2")
    //)

    assertDataFrameEquals(res, expected.selectPP)
  }
}
