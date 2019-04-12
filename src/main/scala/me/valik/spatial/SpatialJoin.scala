package me.valik.spatial

import spatialspark.operator.SpatialOperator

object SpatialJoin {

  /**
    * parse spatial operator name
    *
    * @param predicate one of: withindist, within, contains, intersects, overlaps
    * @return NearestD by default
    */
  def spatialOperator(predicate: String): SpatialOperator.SpatialOperator =
    predicate.toLowerCase match {
      case p if p.contains("withindist") => SpatialOperator.WithinD
      case p if p.contains("within") => SpatialOperator.Within
      case p if p.contains("contains") => SpatialOperator.Contains
      case p if p.contains("intersects") => SpatialOperator.Intersects
      case p if p.contains("overlaps") => SpatialOperator.Overlaps
      case _ => SpatialOperator.NearestD
    }

  def isNearest(op: String): Boolean = spatialOperator(op) == SpatialOperator.NearestD

  def isWithinD(op: String): Boolean = spatialOperator(op) == SpatialOperator.WithinD

  /**
    * Extract distance aka radius from ops like "withindist 10000";
    * roughly convert meters to decimal degrees.
    * 40 km = 1 deg near Salekhard, so to be on a safe side and get a bigger value
    * in degrees, let's say 1 degree = 35 kilometers.
    *
    * You will need to apply more precise filter after join.
    *
    * @return (radius_meters, radius_degrees), 0 by default
    */
  def extractRadius(op: String): Distance = {
    val metersInDeg = 35000d // 35 km in 1 deg
    // try to convert to int the number after space symbol; 0 by default
    import me.valik.toolbox.StringToolbox._
    implicit val sep = Separators(" ")
    val radiusM = op.extractNumber(1).getOrElse(0)

    Distance(radiusM, radiusM / metersInDeg)
  }

  case class Distance(meters: Double, degrees: Double)
}