import sbt._

object Versions {
  val spatialSpark = "spark2.4-scala2.12"
}

object Projects {
  lazy val spatialSpark = RootProject(uri(
    s"https://github.com/vasnake/SpatialSpark.git#${Versions.spatialSpark}"))
}
