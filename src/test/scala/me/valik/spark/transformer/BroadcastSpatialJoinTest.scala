package me.valik.spark.transformer

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row

import me.valik.spark.test._

class BroadcastSpatialJoinTest extends
  FlatSpec with DataFrameSuiteBase {

  def makeTransformer: BroadcastSpatialJoin =
    new BroadcastSpatialJoin()
      .setDataset("poi")
      .setDatasetPoint("lon, lat")

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "smoke"
  it should "pass smoke test" in {
    import spark.implicits._

    val input = Seq(("1", "2019-04-05", "37.6095", "55.9297")).toDF("id", "date", "lon", "lat")
    val expected = Seq(("1", "foo"), ("2", "bar")).toDF("id", "data")

    val transformer = makeTransformer
    val output = transformer.transform(input)

    //output.show(3, truncate=false)
    assertDataFrameEquals(output, expected)
  }
}

class ContainmentJoinDataFrameTest extends
  FlatSpec with Matchers with SimpleLocalSpark with DataFrameTestTools {

  def makeTransformer: BroadcastSpatialJoin =
    new BroadcastSpatialJoin()
      .setDataset("poi")
      .setDatasetPoint("lon, lat")

  // testOnly me.valik.spark.transformer.ContainmentJoinTest -- -z "smoke"
  it should "pass smoke test" in {
    import spark.implicits._

    val input = Seq(("1", "2019-04-05", "37.6095", "55.9297")).toDF("id", "date", "lon", "lat")
    val expected = Seq(("1", "foo"), ("2", "bar")).toDF("id", "data")

    val res = makeTransformer.transform(input)
    // res.show(3, truncate=false)
    res.collect should contain theSameElementsAs Seq(
      Row("1", "foo"),
      Row("2", "bar")
    )

    assertDataFrameEquals(res, expected)
  }
}
