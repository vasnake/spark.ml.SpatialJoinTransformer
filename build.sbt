// me.valik.spark %% spark-transformer-spatialjoin % 0.0.1
// me.valik.spark.transformer.BroadcastSpatialJoin

val Organization = "me.valik.spark"
val Name = "spark-transformer-spatialjoin"
val Version = "0.0.1-SNAPSHOT"

val ScalaVersion = "2.12.8"
val SparkVersion = "2.4.1"

val sparkDeps = Seq(
  //"org.apache.spark" %% "spark-streaming" % SparkVersion,
  //"org.apache.spark" %% "spark-sql" % SparkVersion,
  //"org.apache.spark" %% "spark-hive" % SparkVersion,
  "org.apache.spark" %% "spark-mllib" % SparkVersion,
  "org.apache.spark" %% "spark-core" % SparkVersion
)

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.scalacheck" %% "scalacheck" % "1.14.0",

  // not ready yet: unresolved dependency: com.holdenkarau#spark-testing-base_2.12;2.4.0_0.11.0
  // use local lib (sbt +package) from https://github.com/vasnake/spark-testing-base
   "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0"
)

val spatialDeps = Seq(
  // https://github.com/locationtech/jts
  "org.locationtech.jts" % "jts-core" % "1.16.1"
)

val buildSettings = Seq(
  organization := Organization,
  name := Name,
  version := Version,
  scalaVersion := ScalaVersion,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-encoding", "UTF-8"),
  // assembly parameters
  test in assembly := {},
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(
    includeScala = false, includeDependency = true),
  // temp lib fix
  assemblyMergeStrategy in assembly := {
    case n if n.contains("holdenkarau") => MergeStrategy.discard
    case x => (assemblyMergeStrategy in assembly).value(x)
  }
)

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  Resolver.sonatypeRepo("public")
)

// spark testing tuning
parallelExecution in Test := false
fork := true
concurrentRestrictions in Scope.Global += Tags.limit(Tags.Test, 1)

// project
lazy val root = (project in file(".")).settings(
  buildSettings ++ Seq(
    libraryDependencies ++= sparkDeps.map(_ % Provided)
      ++ spatialDeps
      ++ testDeps.map(_ % Test)
  )

  // no pom publishing
  //pomIncludeRepository := { x => false },

  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  //run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

).dependsOn(Projects.spatialSpark)
