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

    //output.show(20, truncate=false)
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
      //output.show(20, truncate=false)
      assertDataFrameEquals(output, expected.selectCSV(
        "id, lon, lat, poi_id as poi_number, name"))
    }

    val transformer = makeTransformer(data.toDF)
      .setDataColumns("poi_id as poi_number, name as poi_name")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
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
    //output.show(20, truncate=false)
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
    //output.show(20, truncate=false)
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
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV(
      "id, wkt, poi_id"))
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "dataset WKT"
  it should "parse dataset WKT" in {
    import spark.implicits._

    val input = parsePointID(
      """
        |i1, 1.4, 1.4
        |i2, 1.6, 1.6
      """.stripMargin).toDS

    val data = parseWKTPoiID(
      """
        |d1; POLYGON((1 1,2 1,1 2,1 1))
        |d2; POLYGON((2 1,2 2,1 2,2 1))
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1.4, 1.4, d1
        |i2, 1.6, 1.6, d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setDatasetPoint("")
      .setDatasetWKT("wkt")
      .setPredicate("contains") // data polygon contains input point (broadcast input)
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV(
      "id, lon, lat, poi_id"))
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "join selected"
  it should "join selected data columns" in {
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

    def onlyOne = {
      val transformer = makeTransformer(data.toDF)
        .setDataColumns("name")
      val output = transformer.transform(input)
      //output.show(20, truncate=false)
      assertDataFrameEquals(output, expected.selectCSV("id, lon, lat, name"))
    }

    val transformer = makeTransformer(data.toDF)
      .setDataColumns("poi_id, name")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.toDF)

    onlyOne
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "withindist"
  it should "use withindist predicate" in {
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
        |d3, 2.101, 2.101
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setPredicate("withindist 15700")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "intersects"
  it should "use intersects predicate" in {
    import spark.implicits._

    val input = parseWKTPointID(
      """
        |i1; LINESTRING(1 1,2 2)
        |i2; LINESTRING(2 2,3 3)
      """.stripMargin).toDS

    val data = parseWKTPoiID(
      """
        |d1; LINESTRING(2 1,1 2)
        |d2; LINESTRING(3 2,2 3)
      """.stripMargin).toDS

    val expected = parseWKTPointPoi(
      """
        |i1; LINESTRING(1 1,2 2); d1
        |i2; LINESTRING(2 2,3 3); d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setPredicate("intersects")
      .setInputPoint("").setInputWKT("wkt")
      .setDatasetPoint("").setDatasetWKT("wkt")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV("id, wkt, poi_id"))
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "overlaps"
  it should "use overlaps predicate" in {
    import spark.implicits._

    val input = parseWKTPointID(
      """
        |i1; POLYGON((2 4,2 2,4 2,2 4))
        |i2; POLYGON((12 4,12 2,14 2,12 4))
      """.stripMargin).toDS

    val data = parseWKTPoiID(
      """
        |d1; POLYGON((3 1,3 3,1 3,3 1))
        |d2; POLYGON((13 1,13 3,11 3,13 1))
      """.stripMargin).toDS

    val expected = parseWKTPointPoi(
      """
        |i1; POLYGON((2 4,2 2,4 2,2 4)); d1
        |i2; POLYGON((12 4,12 2,14 2,12 4)); d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setPredicate("overlaps")
      .setInputPoint("").setInputWKT("wkt")
      .setDatasetPoint("").setDatasetWKT("wkt")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectCSV("id, wkt, poi_id"))
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "broadcast dataset"
  it should "broadcast dataset" in {
    import spark.implicits._

    val input = parsePointID(
      """
        |i1, 1, 1
        |i2, 2, 2
        |i3, 3, 3
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
        |i3, 3, 3, d2
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setBroadcast("dataset")
      .setPredicate("nearest") // for each input row find nearest point in broadcasted dataset
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "broadcast input"
  it should "broadcast input" in {
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
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
        |i2, 2, 2, d3
      """.stripMargin).toDS

    val transformer = makeTransformer(data.toDF)
      .setBroadcast("input")
      .setPredicate("nearest") // for each dataset row find nearest point in broadcasted input
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "filter"
  it should "apply filter on data loading" in {
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
        |d3, 3.1, 3.1, c
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
      """.stripMargin).toDS

    def noFilter = {
      val expected = parsePointPoi(
        """
          |i1, 1, 1, d1
          |i2, 2, 2, d2
          |i2, 2, 2, d3
        """.stripMargin).toDS
      val transformer = makeTransformer(data.toDF)
      val output = transformer.transform(input)
      //output.show(20, truncate=false)
      assertDataFrameEquals(output, expected.selectPP)
    }

    //by default: for each dataset row find nearest point in broadcasted input
    val transformer = makeTransformer(data.toDF)
      .setDatasetFilter("name in ('a', 'b')")
    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)

    noFilter
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "condition"
  it should "apply extra condition function" in {
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
        |d3, 3.1, 3.1, i2
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
        |i1, 1, 1, d3
      """.stripMargin).toDS

    // by default: for each dataset row find nearest point in broadcasted input;
    val transformer = makeTransformer(data.toDF)
      // right: dataset marked to broadcast, left: dataset to iterate over;
      .setJoinCondition("right.id != left.name") // input.id != dataset.name

    val output = transformer.transform(input)
    //output.show(20, truncate=false)
    assertDataFrameEquals(output, expected.selectPP)

    def noCondition = {
      val expected = parsePointPoi(
        """
          |i1, 1, 1, d1
          |i2, 2, 2, d2
          |i2, 2, 2, d3
        """.stripMargin).toDS
      val transformer = makeTransformer(data.toDF)
      val output = transformer.transform(input)
      //output.show(20, truncate=false)
      assertDataFrameEquals(output, expected.selectPP)
    }
  }

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTest -- -z "schema"
  it should "transform schema" in {
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

    val transformer = makeTransformer(data.toDF)
      .setDataColumns("poi_id as poi_number, name as poi_name")
    val expectedSchema = expected.selectCSV(
      "id, lon, lat, poi_id as poi_number, name as poi_name").schema

    val output = transformer.transformSchema(input.schema)
    assert(output, expectedSchema)
  }

}

object BroadcastSpatialJoinTest {

  case class PointID(id: String, lon: Double, lat: Double)
  case class PoiID(poi_id: String, lon: Double, lat: Double, name: Option[String] = None)
  case class PointPoi(id: String, lon: Double, lat: Double, poi_id: String, name: Option[String] = None)
  case class WKTPointID(id: String, wkt: String)
  case class WKTPoiID(poi_id: String, wkt: String, name: Option[String] = None)
  case class WKTPointPoi(id: String, wkt: String, poi_id: String, name: Option[String] = None)

  // one line example: "d1; POLYGON((1 1,2 1,1 2,1 1))"
  def parseWKTPoiID(text: String): Seq[WKTPoiID] = for {
    Array(k, wkt, rest @ _*) <- parseColumns(text, ";")
  } yield WKTPoiID(k, wkt, rest.headOption)

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
    new me.valik.spark.transformer.BroadcastSpatialJoin()
      .setDataset(name)
      .setDatasetPoint("lon, lat")
      .setInputPoint("lon, lat")
      .setDataColumns("poi_id")
  }

  //val sqltransformer = org.apache.spark.ml.feature.SQLTransformer

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
