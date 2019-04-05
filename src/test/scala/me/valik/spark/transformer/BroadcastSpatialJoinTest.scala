package me.valik.spark.transformer

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession

class BroadcastSpatialJoinTest
  extends FunSuite with SharedSparkContext {
  override def reuseContextIfPossible: Boolean = false

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "smoke"
  test("smoke test") {
    val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate
    import spark.implicits._
    val input = Seq(("1", "2019-04-05", "37.6095", "55.9297")).toDF("id", "date", "lon", "lat")
    val transformer = new BroadcastSpatialJoin()
    val output = transformer.transform(input)
    output.show(3, truncate=false)
    assert(output.collect.sameElements(
      Seq(("1", "foo"), ("2", "bar")).toDF("id", "data").collect
    ))
  }
}
