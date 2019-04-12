package me.valik.spark.geometry

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.DataTypes
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.WKTReader

object DatasetGeometry {

  // TODO: add unit tests

  /**
    * earth model id
    */
  val sridWGS84 = 4326

  /**
    * Parse WKT geometry and return Geometry object or null
    */
  def wkt2geom(wkt: String)(implicit wktReader: WKTReader): Geometry = {
    if (wkt != null && wkt.nonEmpty) wktReader.read(wkt) else null
  }

  case class GeometryMeta(gf: GeometryFactory, wktReader: WKTReader)

  object GeometryMeta {
    def apply(srid: Int): GeometryMeta = {
      val gf = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), srid)
      new GeometryMeta(gf, new WKTReader(gf))
    }
  }

  /**
    * Create RDD by adding Geometry to df Row.
    * Geometry object will be created from df columns using geomSpec interface.
    * n.b. sometimes geometry can be null!
    * @param df input data frame
    * @param geomSpec geometry data specification
    * @return df.rdd.mapPartition ... (row, geometry)
    */
  def addGeometryToRDD(df: DataFrame, geomSpec: DatasetGeometry): RDD[(Row, Geometry)] = {
    import me.valik.spark.geometry.DatasetGeometry._
    // preservesPartitioning = true ? may be, if you can explain why it's important here
    df.rdd.mapPartitions(it => {
      implicit val gm = GeometryMeta(sridWGS84)
      // geometry can be null?
      for (row <- it) yield (row, geomSpec.geometry(row))
    })
  }

  /**
    * abstraction over two types of geometry: from wkt column or from point(lon, lat) columns
    */
  sealed trait DatasetGeometry {
    // TODO: may return mull!
    def geometry(rec: Row)(implicit gm: GeometryMeta): Geometry
    def columns: Seq[Column] // wkt or (lon,lat)
    def colnames: Seq[String]
  }

  case class DatasetGeometryPoint(lon: String, lat: String) extends DatasetGeometry {
    override def geometry(rec: Row)(implicit gm: GeometryMeta): Geometry = {
      gm.gf.createPoint(
        // can be null?
        new Coordinate(rec.getAs[Double](lon), rec.getAs[Double](lat))
      ).asInstanceOf[Geometry]
    }

    override def columns: Seq[Column] = Seq(
      new Column(lon).cast(DataTypes.DoubleType),
      new Column(lat).cast(DataTypes.DoubleType))

    override def colnames = Seq(lon, lat)
  }

  case class DatasetGeometryWKT(wktcol: String) extends DatasetGeometry {
    override def geometry(rec: Row)(implicit gm: GeometryMeta): Geometry = {
      // can be null!
      wkt2geom(rec.getAs[String](wktcol))(gm.wktReader)
    }

    override def columns: Seq[Column] = Seq(new Column(wktcol).cast(DataTypes.StringType))
    override def colnames = Seq(wktcol)
  }

}
