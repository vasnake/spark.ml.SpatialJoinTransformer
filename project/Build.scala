import sbt._

object Versions {
  val spatialSpark = "v1.2.3"
}

object Projects {
  lazy val spatialSpark = RootProject(uri(
    s"https://github.com/vasnake/SpatialSpark.git#${Versions.spatialSpark}"))
}
