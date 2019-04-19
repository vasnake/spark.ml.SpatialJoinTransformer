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
two columns with Longitude and Latitude coordinates.

Another requirement: `extenal dataset` must be registered in Spark SQL metastore/catalog.
It can be Hive table or some previously registered DataFrame.

Shortest possible example: `external dataset` registered with name `poi` that
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

### Transformer parameters
All parameters are String parameters.

condition, setJoinCondition
:  experimental feature, it should be possible to apply extra filter to (input.row, dataset.row) pair
found as a join candidates by spatial relation. e.g. `fulldate between start_ts and end_ts`

filter, setDatasetFilter
:  SQL expression passed to load `dataset` method in case you need to apply filtering before join.

broadcast, setBroadcast
:  which dataset will be broadcasted, two possible values: `input` or `external`,
by default it will be `input`.

predicate, setPredicate
:  one of supported spatial relations: `within`, `contains`, `intersects`, `overlaps`, `nearest`.
By default it will be `nearest`.
Operator `within` should be used in form of `within n` where `n` is a distance parameter in meters.

n.b. `broadcast` and `predicate` closely related: `broadcast` defines a `right` dataset and then
spatial relation can be interpreted as "left contains right" if `predicate` is `contains` for example.

dataset, setDataset
:  external dataset name, should be registered in SQL catalog (metastore).

dataColumns, setDataColumns
:  column names from `dataset` you need to join to `input`.
Format: CSV. Any column could be renamed using alias, using " as alias" form.
For example: `t.setDataColumns("poi_id, name as poi_name"")`

distanceColumnAlias, setDistColAlias
:  if not empty, computable column with defined name will be added to `input`.
That column will contain distance (meters) between centroids of `input` and `dataset` geometries.

datasetWKT, setDatasetWKT
:  external dataset column name, if not empty that column must contain geometry definition in WKT format.

datasetPoint, setDatasetPoint
:  two column names for external dataset, if not empty that columns must contain
Lon, Lat (exactly in that order) coordinates for point geometry.

Same goes for inputWKT, setInputWKT and
inputPoint, setInputPoint

N.b. you should define only one source for geometry objects, it's a WKT or Point, not both.

numPartitions, setNumPartitions
:  repartition parameter, in case if you want to repartition `external dataset`
before join.

## Notes and limitations

Transformer allows you to join input dataset with selected external dataset
using spatial relations between two geometry columns (or four columns in case of
lon, lat points). As any other join, it allows you to add selected columns
(and computable `distance` column) from external dataset to input dataset.

Only inner join implemented for now.

geometry
:  spatial data defined as column containing WKT-formatted primitives: points, polylines, polygons;
WGS84 coordinate system expected (lon,lat decimal degrees GPS coordinates).
Points can be represented as two columns: (lon, lat).

input aka input dataset
:  DataFrame to which transformer is applied, e.g.
`val result = bsj.transform(input)`

dataset aka external dataset aka external
:  DataFrame (table or view) registered in spark sql catalog
(or hive metastore); e.g. `data.createOrReplaceTempView("poi_with_wkt_geometry")`

broadcast aka setBroadcast parameter
:  current limitation is that transformer perform join using the
BroadcastSpatialJoin module that require one of the datasets to be broadcasted.
It means that one of the `input` or `external` datasets must be small enough to be broadcasted by Spark.
By default `input` will be broadcasted and `external` will be iterated using flatMap to find
all the records from `input` that satisfy spatial relation (with `filter` and `condition`).

`broadcast` parameter and `predicate` parameter together defines result of join.
For example, consider input that have two rows (2 points) and dataset that have four rows (4 points).
Let's set predicate to the `nearest`. 
By default, input will be broadcasted and that means that result table will have four rows:
nearest point from input for each point from external dataset.

left or right dataset
:  `broadcast` parameter defines which dataset will be considered `right` and the other, accordingly, `left`.
By default, `input` will be broadcasted, which means, `input` will be the `right` dataset and
`external dataset` will be `left`.

The join process looks like iteration (flatMap) over `left` dataset and, for each left.row 
we search for rows in `right` dataset (after building RTree spatial index)
that satisfy defined conditions (spatial and extra).
In this scenario we need to broadcast the `right` dataset, hence it should be small enough.
As you can see, `broadcast` parameter defines which of two datasets will be `right`
and then another will be `left`.
