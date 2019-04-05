// me.valik.spark %% spark-transformer-spatialjoin % 0.0.1
// me.valik.spark.transformer.BroadcastSpatialJoin

lazy val root = (project in file(".")).settings(
  inThisBuild(List(
    organization := "me.valik.spark",
    scalaVersion := "2.12.8"
  )),
  name := "spark-transformer-spatialjoin",
  version := "0.0.1-SNAPSHOT",

  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  parallelExecution in Test := false,
  fork := true,

  resolvers ++= Seq(
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
    Resolver.sonatypeRepo("public")
  ),

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",

    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"

    // not ready yet: unresolved dependency: com.holdenkarau#spark-testing-base_2.12;2.4.0_0.11.0
    // "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % "test"
  )

  // no pom publishing
  //pomIncludeRepository := { x => false },

  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  //run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

)
