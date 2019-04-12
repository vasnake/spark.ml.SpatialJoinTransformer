//me.valik.spark.transformer.BroadcastSpatialJoin
package me.valik.spark.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

import scala.annotation.elidable
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
  * `left` or `right` dataset: the join process looks like we iterate (flatMap) over `left`
  * dataset and, for each left.row we search for rows in `right` dataset that satisfy
  * some conditions (spatial and extra).
  * In this scenario we need to broadcast the `right` dataset, hence it should be small enough.
  * As you can see, `broadcast` parameter defines which of two datasets will be `right`
  * and then another will be `left`.
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

  @transient private var config: Option[TransformerConfig] = None

  protected def getConfig(spark: SparkSession): TransformerConfig = {
    config.getOrElse({
      config = Some(makeConfig(spark))
      config.get
    })
  }

  protected def loadDataset(name: String, spark: SparkSession): DataFrame = {
    spark sql s"select * from $name"
  }

  import me.valik.toolbox.StringToolbox.{RichString, DefaultSeparators}
  import DefaultSeparators.commaColon

  private def checkParams(): Unit = {
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

  private def makeConfig(spark: SparkSession): TransformerConfig = {
    checkParams()

    def parsePointColumns(str: String) = Try {
      val Array(lon, lat) = str.splitTrim
      PointColumns(lon, lat)
    }.getOrElse(PointColumns("", ""))

    val dataCols: Seq[String] = $(dataColumns).splitTrim

    val ds = { // external dataset, filtered and projected
      val conditionCols: Seq[String] = extraConditionColumns($(condition))
      val cols = (dataCols ++
        Seq($(datasetWKT)) ++
        $(datasetPoint).splitTrim ++
        conditionCols
        ).filter(_.nonEmpty).toSet.toList

      val df: DataFrame = loadDataset($(dataset), spark)
      val fltr = $(filter).trim
      val filtered = if (fltr.nonEmpty) df.filter(fltr) else df

      val projected = filtered.select(cols.head, cols.tail: _*)
      Try {
        projected.repartition($(numPartitions).trim.toInt)
      }.getOrElse(projected)
    }

    val dataColAliases: Seq[String] = {
      val dca = $(dataColumnAliases).splitTrim
      dataCols.zipWithIndex.map { case (name, idx) =>
        // find alias by index or use name as alias
        dca.applyOrElse(idx, (_: Int) => name)
      } }

    TransformerConfig(
      ExternalDatasetConfig(
        name = $(dataset),
        df = ds,
        wktColumn = $(datasetWKT),
        parsePointColumns($(datasetPoint)),
        dataCols,
        dataColAliases),
      InputDatasetConfig(
        wktColumn = $(inputWKT),
        parsePointColumns($(inputPoint))),
      $(distanceColumnAlias),
      spatialPredicate = $(predicate),
      extraPredicate = $(condition),
      broadcastInput = $(broadcast) == input
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

  override def transform(inputDS: Dataset[_]): DataFrame = {
    val spark = inputDS.sparkSession
    spatialJoin(inputDS.toDF, getConfig(spark), spark)
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

  type ExtraConditionFunc = (Row, Row) => Boolean


  /**
    * transformer debug tool
    * @param df dataset to show `df.show(n, truncate)`
    * @param txt message to print before dumping df
    * @param n max number of rows to print
    * @param truncate truncate long lines or not
    */
  @elidable(annotation.elidable.FINE)
  def show(df: DataFrame, txt: String = "spatial-join-debug", n: Int = 7, truncate: Boolean = true): Unit = {
    println(s"msg: `$txt`")
    df.show(n, truncate)
  }

  def spatialJoin(inputDF: DataFrame, config: TransformerConfig, spark: SparkSession): DataFrame = {
    import me.valik.spatial.SpatialJoin._
    //import spark.implicits._
    //import org.locationtech.jts.geom._
    //implicit val gm = {
    //  val gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), sridWGS84)
    //  GeometryMeta(gf, new WKTReader(gf))
    //}

    // geometry interface
    val inputGeom = config.inputCfg.geomSpec
    val dsetGeom = config.datasetCfg.geomSpec
    // data to join
    val needDistance = config.distanceColumnAlias.nonEmpty
    val dataColNames = config.datasetCfg.dataColumns
    // filter by distance needed?
    val filterByDist = isWithinD(config.spatialPredicate)
    val radius = extractRadius(config.spatialPredicate).meters.toInt // n meters or 0

    // from dataset we need (geometry, data, used-in-filter-cols)
    val dataset = {
      val dset = config.datasetCfg.dataset
      val gcols = dsetGeom.colnames.toSet
      val dcols: Set[String] = (dataColNames ++ extraConditionCols(config.extraPredicate, dset)).toSet -- gcols
      val additionalDateCols =
        if (datePruningConfig.isDefined) Seq(DATASET_DATE_COLUMN)
        else Seq()
      val cols = (dcols ++ additionalDateCols).toList.map(col) ++ dsetGeom.columns

      dset.select(cols: _*)
    }

    // from input we need (keys, geometry)
    val input = inputDF.select(inputKeyColNames.map(col) ++ inputGeom.columns: _*)
      .withDateIntervalColumns(datePruningConfig)

    // debug
    show(dataset, s"dataset parts ${dataset.rdd.getNumPartitions}")
    show(input, s"input parts ${input.rdd.getNumPartitions}")

    // extra filter func
    val condition = extraConditionFunc(config.extraPredicate, dataset)

    // join postprocessing: distance, direction, precise filter-by-distance
    def postprocess(dscols: Row, incols: Row, dsgeom: Geometry, ingeom: Geometry
    ): Option[(Row, Row, Double, Boolean)] = {
      lazy val (point, segment) = (Point(ingeom), Segment(dsgeom))

      // calc distance if needed: meters between centroids
      val distance = {
        if (filterByDist || needDistance) {
          if (isSegmentDataset) point2SegmentDistance(point, segment).toDouble
          else geoDistance(dsgeom, ingeom).toDouble
        }
        else 0d
      }

      // calc CW/CCW direction if needed
      def direction = {
        if (needDirection) isSegmentClockwise(point, segment)
        else false
      }

      // filter by distance if required
      if (!filterByDist || distance <= radius) Some((dscols, incols, distance, direction))
      else None
    }

    // do join
    val crosstable = {
      // (dataset keys, geom)
      val ds = dataset.rdd.map { case row: Row => (row, dsetGeom.geometry(row)) }
      // (input keys, geom)
      val inp = input.rdd.map { case row: Row => (row, inputGeom.geometry(row)) }

      // do spatial join, broadcasting dataset or input; compute distance, directions
      spatialJoinWrapper(spark, ds, inp, config.predicate, condition, config.broadcastInput, datePruningConfig)
        .flatMap { case (dscols, incols, dsgeom, ingeom) => postprocess(dscols, incols, dsgeom, ingeom) }
    }

    // columns added after spatial join
    val distColName = "distance"
    val directionColName = "clockwise"

    // convert rdd to dataframe
    val crosstabDf = {
      val fields: Seq[StructField] = input.schema.fields.toSeq ++
        dataset.schema.fields.toSeq :+
        (if (needDistance) StructField(distColName, DataTypes.DoubleType) else null) :+
        (if (needDirection) StructField(directionColName, DataTypes.BooleanType) else null)

      val schema = StructType(fields.filter(_ != null))

      val rdd = crosstable.map { case (dsrow, inprow, dist, dir) => {
        Row.fromSeq(inprow.toSeq ++ dsrow.toSeq ++ Seq(dist, dir))
      }}

      spark.createDataFrame(rdd, schema)
        .dropDateIntervalColumns(datePruningConfig)
    }
    show(crosstabDf, "crosstable")

    // input left-outer-join data

    if (config.aggStatement.isEmpty) {
      // add 'data' and, optionally, distance
      val dcols = dataColNames.zip(config.dataColAlias).map { case (name, alias) =>
        col(s"link.$name") as alias
      }
      val distdir = (
        if (needDistance) Seq(col(s"link.$distColName") as config.distColAlias)
        else Seq.empty) ++ (
        if (needDirection) Seq(col(s"link.$directionColName") as config.clockwiseAlias)
        else Seq.empty)

      inputDF.as("input").join(crosstabDf.as("link"), inputKeyColNames, "left_outer")
        .select((Seq($"input.*") ++ dcols ++ distdir): _*)
    }
    else {
      // add aggregation result, no 'data' or 'distance'
      val folded = contextAggUtils.executeSql(crosstabDf, config.aggStatement, inputKeyColNames)
      val addcols = folded.columns.filter(cn => !inputDF.columns.contains(cn))

      inputDF.as("input").join(folded.as("link"), inputKeyColNames, "left_outer")
        .select("input.*", addcols: _*)
    }
  }

  /**
    * Produce filter function to push down to spatial join.
    * Join direction (broadcast input or external dataset) must be set
    * accordingly to the predicate.
    *
    * @param predicate predefined string, one of:
    *                  {{{
    right.fulldate_ts between left.start_ts and left.end_ts
                       }}}
    * @return function that will be called for each row in left dataset for each
    *         candidate-to-join row from the right dataset
    */
  def extraConditionFunc(predicate: String): Option[ExtraConditionFunc] = {
    parseExtraCondition(predicate).map(_.func)
  }

  /**
    * Get external dataset column names used in extra-condition filter,
    * see {@link extraConditionFunc}
    *
    * @param predicate predefined string
    * @return list of column names
    */
  def extraConditionColumns(predicate: String): Seq[String] = {
    parseExtraCondition(predicate).map(_.columns).getOrElse(Seq.empty)
  }

  protected def parseExtraCondition(predicate: String): Option[ExtraCondition] = {
    // TODO: parse sql-like statement and produce function dynamically
    predicate.toLowerCase match {
      case "right.fulldate_ts between left.start_ts and left.end_ts" => Some(
        ExtraCondition(
          columns = Seq("start_ts", "end_ts"),
          func = (left, right) => {
            val ut = right.getAs[Int]("fulldate_ts")
            val bts = left.getAs[Long]("start_ts")
            val ets = left.getAs[Long]("end_ts")
            bts <= ut && ut <= ets
          }) )
      case "" => None
      case x => throw new IllegalArgumentException(s"Spatial join transformer error: unknown extra condition `$x`")
    }
  }

  case class ExtraCondition(columns: Seq[String], func: ExtraConditionFunc)

  case class TransformerConfig(
    datasetCfg: ExternalDatasetConfig,
    inputCfg: InputDatasetConfig,
    distanceColumnAlias: String,
    spatialPredicate: String,
    extraPredicate: String,
    broadcastInput: Boolean
  )

  sealed trait DatasetConfig {
    def isWKT: Boolean = wktColumn.nonEmpty
    def wktColumn: String
    def pointColumns: PointColumns

    import me.valik.spark.geometry.DatasetGeometry._

    def geomSpec: DatasetGeometry = {
      if (isWKT) DatasetGeometryWKT(wktColumn)
      else DatasetGeometryPoint(pointColumns.lon, pointColumns.lat)
    }
  }

  case class ExternalDatasetConfig(
    name: String,
    df: DataFrame,
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
