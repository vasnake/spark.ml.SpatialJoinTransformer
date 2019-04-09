//me.valik.spark.transformer.BroadcastSpatialJoin
package me.valik.spark.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}

/**
  * spark.ml.transformer that join input dataframe with selected external dataset
  * using spatial relations between two geometry columns.
  * Allows you to add selected columns (and `distance` column)
  * from external dataset to input dataset.
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
  *
  * @param uid pipeline stage id
  */
class BroadcastSpatialJoin(override val uid: String) extends
  Transformer with Params with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("spatial_join"))

  import me.valik.spark.transformer.BroadcastSpatialJoin._

  // parameters

  // setJoinCondition("fulldate between start_ts and end_ts")
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

  final val dataColumns = new Param[String](this, "dataColumns", "external ds column names to join to input, csv")
  setDefault(dataColumns, "")
  def setDataColumns(value: String): this.type = set(dataColumns, value)

  final val dataColAlias = new Param[String](this, "dataColAlias", "aliases for added data cols, csv")
  setDefault(dataColAlias, "")

  def setDataColAlias(value: String) = set(dataColAlias, value)

  final val distColAlias = new Param[String](this, "distColAlias", "alias for added distance col")
  setDefault(distColAlias, "")
  def setDistColAlias(value: String) = set(distColAlias, value)

  /**
    * Define segment direction around location point
    */
  final val clockwiseColAlias = new Param[String](this, "clockwiseColAlias", "alias for added is_clockwise col")
  setDefault(clockwiseColAlias, "")
  def setClockwiseColAlias(alias: String) = set(clockwiseColAlias, alias)

  /**
    * Geometry column name in external DS
    */
  final val datasetWkt = new Param[String](this, "datasetWkt", "external ds wkt column name")
  setDefault(datasetWkt, "")
  def setDatasetWkt(value: String) = set(datasetWkt, value)

  // Dataset column names: lon, lat

  @deprecated("use datasetPoint", "0.0.24")
  final val dsetlon: Param[String] = new Param[String](this, "dsetlon", "dataset lon column name")

  @deprecated("use datasetPoint", "0.0.24")
  final val dsetlat: Param[String] = new Param[String](this, "dsetlat", "dataset lat column name")

  setDefault(dsetlon, "")
  setDefault(dsetlat, "")

  @deprecated("use setDatasetPoint", "0.0.24")
  def setDatasetLon(value: String) = set(dsetlon, value)

  @deprecated("use setDatasetPoint", "0.0.24") //"-Xfatal-warnings",
  def setDatasetLat(value: String) = set(dsetlat, value)

  /**
    * Dataset geometry columns, in case it's point (lon, lat)
    */
  final val datasetPoint: Param[String] = new Param[String](this, "datasetPoint", "dataset point columns")
  setDefault(datasetPoint, "")
  def setDatasetPoint(colnames: String) = set(datasetPoint, colnames)

  /**
    * Dataset geometry columns, in case it's linear segments (lon1, lat1, lon2, lat2)
    */
  final val datasetSegment: Param[String] = new Param[String](this, "datasetSegment", "dataset segment columns")
  setDefault(datasetSegment, "")
  def setDatasetSegment(colnames: String) = set(datasetSegment, colnames)

  /**
    * Input ds column name with geometry WKT
    */
  final val inputWkt = new Param[String](this, "inputWkt", "input ds geometry wkt column name")
  setDefault(inputWkt, "")

  def setInputWkt(value: String) = set(inputWkt, value)

  /**
    * Input ds column names: "lon, lat"
    */
  final val lon: Param[String] = new Param[String](this, "lon", "locations lon column name")
  final val lat: Param[String] = new Param[String](this, "lat", "locations lat column name")
  setDefault(lon, "")
  setDefault(lat, "")

  def setInputLon(value: String) = set(lon, value)

  def setInputLat(value: String) = set(lat, value)

  /**
    * input ds column names: "loc_id, fulldate"
    */
  final val inputKeys = new Param[String](this, "inputKeys", "input ds key columns, e.g. 'loc_id, request_date'")
  setDefault(inputKeys, "")

  def setInputKeys(value: String) = set(inputKeys, value)

  final val numPartitions = new Param[String](this, "numPartitions", "dataset repartition parameter, no repartition if empty")
  setDefault(numPartitions, "")

  def setNumPartitions(value: String) = set(numPartitions, value)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /**
    * You should call it before starting heavy and long transform
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
}
