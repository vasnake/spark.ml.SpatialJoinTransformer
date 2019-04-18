package me.valik.spark.transformer

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class BroadcastSpatialJoinTest extends
  FlatSpec with DataFrameSuiteBase {

  import BroadcastSpatialJoinTest._

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "smoke"
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

    val transformer = makeTransformer(data.toDF)
    val output = transformer.transform(input) // .persist(StorageLevel.MEMORY_ONLY)

    output.show(3, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "aliases"
  it should "rename selected data columns (aliases)" in {
    import spark.implicits._

    val input = parsePointID(
      """
        |i1, 1, 1
        |i2, 2, 2
      """.stripMargin).toDS

    val data = parsePoiID(
      """
        |d1, 1.1, 1.1, a
        |d2, 2.1, 2.1, b
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1, a
        |i2, 2, 2, d2, b
      """.stripMargin).toDS

    def aliasForOne = {
      val transformer = makeTransformer(data.toDF)
        .setDataColumns("poi_id as poi_number, name")
      val output = transformer.transform(input)

      output.show(3, truncate=false)
      assertDataFrameEquals(output, expected.selectCSV(
        "id, lon, lat, poi_id as poi_number, name"))
    }

    val transformer = makeTransformer(data.toDF)
      .setDataColumns("poi_id as poi_number, name as poi_name")
    val output = transformer.transform(input)

    output.show(3, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV(
      "id, lon, lat, poi_id as poi_number, name as poi_name"))

    aliasForOne
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "distance"
  it should "add distance column" in {
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
        |i1, 1, 1, d1, 15689
        |i2, 2, 2, d2, 15685
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setDistColAlias("distance")
    val output = transformer.transform(input)

    output.show(3, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV(
      "id, lon, lat, poi_id, int(name) as distance"))
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "repartition"
  it should "repartition external dataset" in {
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
        |d3, 3.1, 3.1
        |d4, 4.1, 4.1
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setNumPartitions("4")
    val output = transformer.transform(input)

    output.show(20, truncate=false)
    assert(output.rdd.getNumPartitions === 4)
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "input WKT"
  it should "parse input WKT" in {
    import spark.implicits._

    val input = parseWKTPointID(
      """
        |i1; POLYGON((1 1,2 1,1 2,1 1))
        |i2; POLYGON((2 1,2 2,1 2,2 1))
      """.stripMargin).toDS

    val data = parsePoiID(
      """
        |d1, 1.4, 1.4
        |d2, 1.6, 1.6
      """.stripMargin).toDS

    val expected = parseWKTPointPoi(
      """
        |i1; POLYGON((1 1,2 1,1 2,1 1)); d1
        |i2; POLYGON((2 1,2 2,1 2,2 1)); d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setInputPoint("")
      .setInputWKT("wkt")
      .setPredicate("within") // data point within input polygon (broadcast input)
    val output = transformer.transform(input)

    output.show(3, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV(
      "id, wkt, poi_id"))
  }

  // TODO:
  //  datasetPoint/datasetWKT;
  //  dataColumns (1,2,...)
  //  predicate: withindist, +within, contains, intersects, overlaps, +nearest;
  //  broadcast: input, dataset;
  //  filter;
  //  condition
  //  transformSchema
}

object BroadcastSpatialJoinTest {
  case class PointID(id: String, lon: Double, lat: Double)
  case class WKTPointID(id: String, wkt: String)
  case class PoiID(poi_id: String, lon: Double, lat: Double, name: Option[String] = None)
  case class PointPoi(id: String, lon: Double, lat: Double, poi_id: String, name: Option[String] = None)
  case class WKTPointPoi(id: String, wkt: String, poi_id: String, name: Option[String] = None)

  // one line example: "i1; POLYGON((1 1,2 1,1 2,1 1))"
  def parseWKTPointID(text: String): Seq[WKTPointID] = for {
    Array(k, wkt) <- parseColumns(text, ";").take(2)
  } yield WKTPointID(k, wkt)

  // one line example: "i1; POLYGON((1 1,2 1,1 2,1 1)); d1"
  def parseWKTPointPoi(text: String): Seq[WKTPointPoi] = for {
    Array(k, wkt, d, rest @ _*) <- parseColumns(text, ";")
  } yield WKTPointPoi(k, wkt, d, rest.headOption)

  // one line example: "i1, 1, 1"
  def parsePointID(text: String): Seq[PointID] = for {
    Array(k, x, y) <- parseColumns(text).take(3)
  } yield PointID(k, x.toDouble, y.toDouble)

  // one line example: "d2, 2.1, 2.1"
  def parsePoiID(text: String): Seq[PoiID] = for {
    Array(k, x, y, rest @ _*) <- parseColumns(text)
  } yield PoiID(k, x.toDouble, y.toDouble, rest.headOption)

  // one line example: "i1, 1, 1, d1, 15689"
  def parsePointPoi(text: String): Seq[PointPoi] = for {
    Array(k, x, y, d, rest @ _*) <- parseColumns(text)
  } yield PointPoi(k, x.toDouble, y.toDouble, d, rest.headOption)

  def makeTransformer(data: DataFrame, name: String = "poi") = {
    data.createOrReplaceTempView(name)
    new BroadcastSpatialJoin()
      .setDataset(name)
      .setDatasetPoint("lon, lat")
      .setInputPoint("lon, lat")
      .setDataColumns("poi_id")
  }

  import scala.language.implicitConversions
  import me.valik.toolbox.StringToolbox._
  import DefaultSeparators.stringToSeparators

  private def parseColumns(text: String, sep: String = ",") =
    text.splitTrim("\n").map(_.splitTrim(sep))

  implicit class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def selectCSV(csv: String): DataFrame = {
      val cols = csv.splitTrim(",")
      ds.selectExpr(cols: _*)
    }

    def selectPP: DataFrame = selectCSV("id, lon, lat, poi_id")
  }
}
