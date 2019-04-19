# spark.ml.SpatialJoinTransformer

[![Build Status](https://travis-ci.com/vasnake/spark.ml.SpatialJoinTransformer.svg?branch=master)](https://travis-ci.com/vasnake/spark.ml.SpatialJoinTransformer)

It is a [spark.ml.Transformer](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.Transformer)
that joins input dataset with external data using
Spatial Relations Predicates.

To perform spatial join, [SpatialSpark](https://github.com/vasnake/SpatialSpark)
BroadcastSpatialJoin object is used.
Also, SpatialJoinTransformer depends on
[LocationTech JTS](https://github.com/locationtech/jts) 
and [GeographicLib](https://sourceforge.net/projects/geographiclib/)

Was built and tested with Spark 2.4 and Scala 2.12

## Installation

You can use binary packages from 
[releases](https://github.com/vasnake/spark.ml.SpatialJoinTransformer/releases)
page or, add dependency to your sbt project:

```scala
// project/Build.scala
object Projects {
  lazy val spatialJoinTransformer = RootProject(uri(
    "https://github.com/vasnake/spark.ml.SpatialJoinTransformer.git#v0.0.1"))
}

// build.sbt
lazy val root = (project in file(".")).settings(
  ???
).dependsOn(Projects.spatialJoinTransformer)

```

May be later I will consider publishing packages to some public repository.
Stay tuned.

## Usage

Let's say, we have an `input` dataset that needs to be transformed, 
and some `external dataset` aka just `dataset`.
To perform a transformation, spatial join exactly, we need these datasets
to have spatial information.
Each dataset have to have a column with WKT geometries or, in case of points,
two columns with Longitude and Latitude coordinates of points.

Another requirement: `extenal dataset` must be registered in Spark SQL metastore/catalog.
It can be Hive table or some previously registered view/table.

Shortest possible example: `external dataset` registered with name `poi` and
has point geometry columns: `lon`, `lat`.
Dataset to transform, `input` also has point geometry, also located in columns `lon` and `lat`:

```scala
val input: DataFrame = ???
val data: DataFrame = ???
data.createOrReplaceTempView("poi")
val transformer = new BroadcastSpatialJoin()
    .setDataset("poi")
    .setDatasetPoint("lon, lat")
    .setInputPoint("lon, lat")
    .setDataColumns("poi_id")
val res = transformer.transform(input)
```

By default predicate `nearest` will be used as spatial relation and, attention, `input`
dataset will be broadcasted. It means that for each row from `poi`, nearest point
from `input` will be found and `poi_id` attribute will be joined to that `input` row.

More detailed examples with different parameters, conditions and predicates you can find in
[tests](https://github.com/vasnake/spark.ml.SpatialJoinTransformer/blob/master/src/test/scala/me/valik/spark/transformer/BroadcastSpatialJoinTest.scala)

More information about Spark transformers you can find in
[documentation](https://spark.apache.org/docs/latest/ml-pipeline.html)

## Notes and limitations
