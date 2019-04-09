//me.valik.spark.transformer.BroadcastSpatialJoin
package me.valik.spark.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}

import scala.util.Try

/**
  * spark.ml.transformer that join input dataframe with selected external dataset
  * using spatial relations between two geometry columns.
  * Allows you to add selected columns (and `distance` column)
  * from external dataset to input dataset.
  * Only inner join implemented for now.
  *
  * <br/><br/>
  * `input` aka `input dataset`: DataFrame to which transformer is applied, e.g.
  * result = bsj.transform(input)
  * <br/><br/>
  * `dataset` aka `external dataset`: DataFrame (table or view) registered in spark sql metastore
  * (or hive metastore); e.g. data.createOrReplaceTempView("poi_with_wkt_geometry")
  * <br/><br/>
  * `broadcast`, `setBroadcast`: current limitation is that transformer using the BroadcastSpatialJoin module
  * required that one of the datasets must be broadcasted. It means that `input` or `external`
  * must be small enough to be broadcasted by spark.
  * By default `input` will be broadcasted and `external` will be iterated using flatMap to find
  * all the records from `input` that satisfy spatial relation (with `filter` and `condition`)
  * <br/><br/>
  * `geometry`: spatial data defined as column containing WKT-formatted primitives: points, polylines, polygons;
  * WGS84 coordinate system expected (lon,lat decimal degrees GPS coordinates).
  * Points can be represented as two columns: (lon, lat).
  *
  * @param uid pipeline stage id
  */
class BroadcastSpatialJoin(override val uid: String) extends
  Transformer with Params with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("spatial_join"))

  import me.valik.spark.transformer.BroadcastSpatialJoin._

  // parameters

  /**
    * experimental feature: extra condition applied to joining records;
    * e.g. `fulldate between start_ts and end_ts`
    */
  final val condition = new Param[String](this, "condition", "extra predicate to push into SpatialJoin")
  setDefault(condition, "")
  def setJoinCondition(value: String): this.type = set(condition, value)

  final val filter = new Param[String](this, "filter", "dataset filter")
  setDefault(filter, "")
  def setDatasetFilter(value: String): this.type = set(filter, value)

  final val broadcast = new Param[String](this, "broadcast", "which DF will be broadcasted: 'input' or 'external' ")
  setDefault(broadcast, input)
  def setBroadcast(value: String): this.type = set(broadcast, value)

  final val predicate = new Param[String](this, "predicate", "spatial op: within, contains, intersects, overlaps, nearest")
  setDefault(predicate, nearest)
  def setPredicate(value: String): this.type = set(predicate, value)

  final val dataset = new Param[String](this, "dataset", "external dataset name, should be registered in sql metastore")
  setDefault(dataset, "")
  def setDataset(value: String): this.type = set(dataset, value)

  final val dataColumns = new Param[String](this, "dataColumns", "external ds column names to join to input, in csv format")
  setDefault(dataColumns, "")
  def setDataColumns(value: String): this.type = set(dataColumns, value)

  final val dataColumnAliases = new Param[String](this, "dataColumnAliases", "aliases for added data columns, in csv format")
  setDefault(dataColumnAliases, "")
  def setDataColAlias(value: String): this.type = set(dataColumnAliases, value)

  final val distanceColumnAlias = new Param[String](this, "distanceColumnAlias", "alias for added distance column")
  setDefault(distanceColumnAlias, "")
  def setDistColAlias(value: String): this.type = set(distanceColumnAlias, value)

  /**
    * Geometry column name in external DS
    */
  final val datasetWKT = new Param[String](this, "datasetWKT", "external ds geometry column name")
  setDefault(datasetWKT, "")
  def setDatasetWKT(value: String): this.type = set(datasetWKT, value)

  /**
    * Dataset geometry columns, in case it's a point (lon, lat)
    */
  final val datasetPoint: Param[String] = new Param[String](this, "datasetPoint", "external dataset point columns, e.g. `lon, lat`")
  setDefault(datasetPoint, "")
  def setDatasetPoint(colnames: String): this.type = set(datasetPoint, colnames)

  /**
    * Input DS column name with geometry WKT
    */
  final val inputWKT = new Param[String](this, "inputWKT", "input ds geometry column name")
  setDefault(inputWKT, "")
  def setInputWKT(value: String): this.type = set(inputWKT, value)

  /**
    * Input DS point geometry columns: "lon, lat"
    */
  final val inputPoint: Param[String] = new Param[String](this, "inputPoint", "input point columns, e.g. `lon, lat`")
  setDefault(inputPoint, "")
  def setInputPoint(colnames: String): this.type = set(inputPoint, colnames)

  final val numPartitions = new Param[String](this, "numPartitions", "external dataset repartition parameter, no repartition if empty")
  setDefault(numPartitions, "")
  def setNumPartitions(value: String): this.type = set(numPartitions, value)

  // config

  private lazy val config: TransformerConfig = getConfig

  import me.valik.toolbox.StringToolbox.{RichString, DefaultSeparators}
  import DefaultSeparators.oneSeparator

  private def checkConfig(): Unit = {
    val datasetNonEmptyGeometries = Seq(
      $(datasetPoint).nonEmpty,
      $(datasetWKT).nonEmpty
    )

    require(datasetNonEmptyGeometries.count(identity) == 1,
      "You must specify one and only one property of (datasetWKT, datasetPoint)")

    require($(datasetPoint).isEmpty || $(datasetPoint).splitTrim.length == 2,
      "datasetPoint property should be empty or contain string like 'lon, lat'")

    // TODO: check other parameters
  }

  private def getConfig: TransformerConfig = {
    checkConfig()

    val dataCols: Seq[String] = $(dataColumns).splitTrim

    val dataColAliases: Seq[String] = {
      val dca = $(dataColumnAliases).splitTrim
      dataCols.zipWithIndex.map { case (name, idx) =>
        dca.applyOrElse(idx, _ => name)
      }
    }

    val df = loadDataset(datasetName)
    val filterCols: Seq[String] = extraConditionCols($(condition), df)

    val ds = {
      val fltr = $(filter).trim
      val cols = (dataCols ++
        Seq($(datasetWkt), $(dsetlon), $(dsetlat)) ++
        $(datasetSegment).splitTrim ++
        $(datasetPoint).splitTrim ++
        filterCols
        ).filterNot(_.isEmpty).toSet.toList

      val filtered = if (!fltr.isEmpty) df.filter(fltr) else df
      val projected = filtered.select(cols.head, cols.tail: _*)
      Try {
        projected.repartition($(numPartitions).trim.toInt)
      }.getOrElse(projected)
    }

    val pointCols = {
      if ($(datasetPoint).splitTrim.size != 2) $(dsetlon) + "," + $(dsetlat)
      else $(datasetPoint)
    }

    SpatialJoinTransformerConfig(
      SpatialJoinTransformerDatasetConfig(
        datasetName,
        ds,
        $(datasetWkt),
        PointColumns(pointCols),
        SegmentColumns($(datasetSegment)),
        dataCols),
      SpatialJoinTransformerInputConfig(
        $(inputWkt), $(lon), $(lat),
        $(inputKeys).splitTrim),
      dataColsAlias,
      $(distColAlias),
      $(clockwiseColAlias),
      $(predicate),
      $(condition),
      $(aggstatement),
      $(broadcastSrc) == input,
      parsedInputDateColumn,
      $(intervalStartOffset),
      $(intervalEndOffset)
    )
  }

  // transformer

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /**
    * You should call it to check schema before starting heavy and long transform
    * @param schema input schema
    * @return output schema
    */
  override def transformSchema(schema: StructType): StructType = {
    val spark = SparkSession.builder.getOrCreate
    // TODO: you should analyze transformer parameters and evaluate output schema
    val emptyRows = spark.sparkContext.emptyRDD[Row]
    val emptyInput = spark.createDataFrame(emptyRows, schema)

    transform(emptyInput).schema
  }

  override def transform(input: Dataset[_]): DataFrame = {
    val spark = input.sparkSession
    import spark.implicits._

    ???
  }

}

object BroadcastSpatialJoin extends DefaultParamsReadable[BroadcastSpatialJoin] {

  /**
    * constant that defines default direction of spatial join: 'input' means
    * that transformer input will be broadcasted by spatial join and dataset will be iterated
    */
  val input = "input"

  /**
    * spatial op, one of: within, contains, intersects, overlaps, nearest
    */
  val nearest = "nearest"

  case class TransformerConfig(
    datasetCfg: ExternalDatasetConfig,
    inputCfg: InputDatasetConfig,
    distanceColumnAlias: String,
    spatialPredicate: String,
    extraPredicate: String,
    broadcastInput: Boolean
  )

  abstract class DatasetConfig {
    def isWKT: Boolean = wktColumn.nonEmpty
    def wktColumn: String
    def pointColumns: PointColumns
  }

  case class ExternalDatasetConfig(
    name: String,
    df: DataFrame,
    numPartitions: Int,
    filter: String,
    wktColumn: String,
    pointColumns: PointColumns,
    dataColumns: Seq[String],
    aliases: Seq[String]
  ) extends DatasetConfig

  case class InputDatasetConfig(
    wktColumn: String,
    pointColumns: PointColumns,
  ) extends DatasetConfig

  case class PointColumns(lon: String, lat: String) {
    def isEmpty: Boolean = lon.isEmpty || lat.isEmpty
  }

}
