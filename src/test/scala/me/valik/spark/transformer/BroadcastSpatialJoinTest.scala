package me.valik.spark.transformer

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

object BroadcastSpatialJoinTest {
  case class PointID(id: String, lon: Double, lat: Double)
  case class PoiID(poi_id: String, lon: Double, lat: Double)
  case class PointPoi(id: String, lon: Double, lat: Double, poi_id: String)

  import scala.language.implicitConversions
  import me.valik.toolbox.StringToolbox._
  implicit def stringToSeparators(sep: String): Separators = Separators(sep)

  private def parseColumns(text: String) = text.splitTrim("\n").map(_.splitTrim(","))

  def parsePointID(text: String): Seq[PointID] = for {
    Array(k, x, y) <- parseColumns(text).take(3)
  } yield PointID(k, x.toDouble, y.toDouble)

  def parsePoiID(text: String): Seq[PoiID] = for {
    Array(k, x, y) <- parseColumns(text).take(3)
  } yield PoiID(k, x.toDouble, y.toDouble)

  def parsePointPoi(text: String): Seq[PointPoi] = for {
    Array(k, x, y, d) <- parseColumns(text).take(4)
  } yield PointPoi(k, x.toDouble, y.toDouble, d)

  def makeTransformer(data: DataFrame, name: String = "poi") = {
    data.createOrReplaceTempView(name)
    new BroadcastSpatialJoin()
      .setDataset(name)
      .setDatasetPoint("lon, lat")
      .setInputPoint("lon, lat")
      .setDataColumns("poi_id")
  }

}

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
    //output.show(3, truncate=false)
    assertDataFrameEquals(output, expected.toDF)
  }

  // TODO:
  //  col.aliases ([not]full set);
  //  add distance ([no]alias);
  //  num.partitions;
  //  inputPoint/inputWKT;
  //  datasetPoint/datasetWKT;
  //  dataColumns (1,2,...)
  //  predicate: withindist, within, contains, intersects, overlaps, nearest;
  //  broadcast: input, dataset;
  //  filter;
  //  condition
}
