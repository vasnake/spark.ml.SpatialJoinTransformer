import sbt._

object Versions {
  val spatialSpark = "broadcast-spatial-join"
}

object Projects {
  lazy val spatialSpark = RootProject(uri(
    s"https://github.com/vasnake/SpatialSpark.git#${Versions.spatialSpark}"))
}
