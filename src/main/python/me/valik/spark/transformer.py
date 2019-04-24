# -*- coding: utf-8 -*-

from pyspark import keyword_only
from pyspark.ml.common import inherit_doc
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer

# from pyspark.ml.feature import SQLTransformer

__all__ = ['BroadcastSpatialJoin']


@inherit_doc
class BroadcastSpatialJoin(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Implements the transforms which are defined as spatial join.

    >>> from me.valik.spark.transformer import BroadcastSpatialJoin
    >>> poi = spark.createDataFrame([("a", 1.1, 3.1), ("b", 2.1, 5.1)], ["poi_id", "lon", "lat"])
    >>> poi.createOrReplaceTempView("poi")
    >>> df = spark.createDataFrame([(0, 1.0, 3.0), (2, 2.0, 5.0)], ["id", "lon", "lat"])
    >>> trans = BroadcastSpatialJoin(
    ...     dataset="poi", dataColumns="poi_id", datasetPoint="lon, lat",
    ...     inputPoint="lon, lat")
    >>> trans.transform(df).head()
    Row(id=0, lon=1.0, lat=3.0, poi_id="a")
    >>> save_path = "/tmp/spatial-join-transformer"
    >>> trans.save(save_path)
    >>> loadedTrans = BroadcastSpatialJoin.load(save_path)
    >>> loadedTrans.transform(df).collect() == trans.transform(df).collect()
    True
    """

    _fqdn = "me.valik.spark.transformer.BroadcastSpatialJoin"

    dataColumns = Param(Params._dummy(),
                       "dataColumns",
                       "external ds column names to join to input, in csv format",
                       typeConverter=TypeConverters.toString)

    datasetWKT = Param(Params._dummy(),
                       "datasetWKT",
                       "external geometry column name",
                       typeConverter=TypeConverters.toString)

    datasetPoint = Param(Params._dummy(),
                         "datasetPoint",
                         "external dataset point columns, e.g. `lon, lat`",
                         typeConverter=TypeConverters.toString)

    inputWKT = Param(Params._dummy(),
                     "inputWKT",
                     "input geometry column name",
                     typeConverter=TypeConverters.toString)

    inputPoint = Param(Params._dummy(),
                       "inputPoint",
                       "input point columns, e.g. `lon, lat`",
                       typeConverter=TypeConverters.toString)

    dataset = Param(Params._dummy(),
                    "dataset",
                    "external dataset name, should be registered in sql metastore",
                    typeConverter=TypeConverters.toString)

    predicate = Param(Params._dummy(),
                      "predicate",
                      "spatial op, one of: withindist, within, contains, intersects, overlaps, nearest",
                      typeConverter=TypeConverters.toString)

    numPartitions = Param(Params._dummy(),
                          "numPartitions",
                          "external dataset repartition parameter, no repartition if empty",
                          typeConverter=TypeConverters.toString)

    distanceColumnAlias = Param(Params._dummy(),
                                "distanceColumnAlias",
                                "alias for added `distance` column",
                                typeConverter=TypeConverters.toString)

    broadcast = Param(Params._dummy(),
                      "broadcast",
                      "which DF will be broadcasted: 'input' or 'external' ",
                      typeConverter=TypeConverters.toString)

    filter = Param(Params._dummy(),
                   "filter",
                   "dataset filter",
                   typeConverter=TypeConverters.toString)

    condition = Param(Params._dummy(),
                      "condition",
                      "extra predicate to push into SpatialJoin",
                      typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self,
                 dataColumns="",
                 distanceColumnAlias="",
                 datasetWKT="",
                 datasetPoint="",
                 inputWKT="",
                 inputPoint="",
                 dataset="",
                 predicate="nearest",
                 broadcast="input",
                 numPartitions="",
                 filter="",
                 condition=""):
        super(BroadcastSpatialJoin, self).__init__()
        self._java_obj = self._new_java_obj(self._fqdn, self.uid)

        # set params default values
        self._setDefault(dataColumns="",
                         distanceColumnAlias="",
                         datasetWKT="",
                         datasetPoint="",
                         inputWKT="",
                         inputPoint="",
                         dataset="",
                         predicate="nearest",
                         broadcast="input",
                         numPartitions="",
                         filter="",
                         condition="")

        # set params init values
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self,
                  dataColumns="",
                  distanceColumnAlias="",
                  datasetWKT="",
                  datasetPoint="",
                  inputWKT="",
                  inputPoint="",
                  dataset="",
                  predicate="nearest",
                  broadcast="input",
                  numPartitions="",
                  filter="",
                  condition=""):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setJoinCondition(self, value):
        return self._set(condition=value)

    def getJoinCondition(self):
        return self.getOrDefault(self.condition)

    def setDatasetFilter(self, value):
        return self._set(filter=value)

    def getDatasetFilter(self):
        return self.getOrDefault(self.filter)

    def setBroadcast(self, value):
        return self._set(broadcast=value)

    def getBroadcast(self):
        return self.getOrDefault(self.broadcast)

    def setDistColAlias(self, value):
        return self._set(distanceColumnAlias=value)

    def getDistColAlias(self):
        return self.getOrDefault(self.distanceColumnAlias)

    def setNumPartitions(self, value):
        return self._set(numPartitions=value)

    def getNumPartitions(self):
        return self.getOrDefault(self.numPartitions)

    def setDataColumns(self, value):
        return self._set(dataColumns=value)

    def getDataColumns(self):
        return self.getOrDefault(self.dataColumns)

    def setDatasetWKT(self, value):
        return self._set(datasetWKT=value)

    def getDatasetWKT(self):
        return self.getOrDefault(self.datasetWKT)

    def setDatasetPoint(self, value):
        return self._set(datasetPoint=value)

    def getDatasetPoint(self):
        return self.getOrDefault(self.datasetPoint)

    def setInputWKT(self, value):
        return self._set(inputWKT=value)

    def getInputWKT(self):
        return self.getOrDefault(self.inputWKT)

    def setInputPoint(self, value):
        return self._set(inputPoint=value)

    def getInputPoint(self):
        return self.getOrDefault(self.inputPoint)

    def setDataset(self, value):
        return self._set(dataset=value)

    def getDataset(self):
        return self.getOrDefault(self.dataset)

    def setPredicate(self, value):
        return self._set(predicate=value)

    def getPredicate(self):
        return self.getOrDefault(self.predicate)
