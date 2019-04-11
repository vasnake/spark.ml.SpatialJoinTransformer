package me.valik.spark.geometry

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.DataTypes

import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
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
