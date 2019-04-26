/*
 * Copyright 2019 Valentin Fedulov <vasnake@gmail.com>
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package me.valik.spatial

import org.locationtech.jts.geom.Geometry
import spatialspark.operator.SpatialOperator

// TODO: add unit tests

/**
  * Spatial join helpers
  */
object SpatialJoin {

  /**
    * get Geodesic.distance between g1.centroid and g2.centroid, meters
    */
  def geoDistance(g1: Geometry, g2: Geometry): Int = {
    import net.sf.geographiclib.Geodesic
    val (p1, p2) = (g1.getCentroid, g2.getCentroid)

    math.round(
      Geodesic.WGS84.Inverse(p1.getY, p1.getX, p2.getY, p2.getX)
        .s12
    ).toInt
  }

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
    val radiusM = op.extractNumber(1).getOrElse(0d)

    Distance(radiusM, radiusM / metersInDeg)
  }

  case class Distance(meters: Double, degrees: Double)
}
