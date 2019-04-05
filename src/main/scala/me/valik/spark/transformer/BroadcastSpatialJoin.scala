//me.valik.spark.transformer.BroadcastSpatialJoin
package me.valik.spark.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}

class BroadcastSpatialJoin(override val uid: String)
  extends Transformer with Params with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("spatial_join"))

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
    Seq(("1", "foo"), ("2", "bar")).toDF("id", "data")
  }

}

object BroadcastSpatialJoin extends DefaultParamsReadable[BroadcastSpatialJoin] {

}
