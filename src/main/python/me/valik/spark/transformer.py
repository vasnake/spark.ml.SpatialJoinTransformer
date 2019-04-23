# -*- coding: utf-8 -*-

from pyspark import keyword_only
from pyspark.ml.common import inherit_doc
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer

# from pyspark.ml.feature import SQLTransformer


@inherit_doc
class BroadcastSpatialJoin(JavaTransformer, JavaMLReadable, JavaMLWritable):
    _fqdn = "me.valik.spark.transformer.BroadcastSpatialJoin"
    predicate = Param(Params._dummy(), "predicate", "spatial op, one of: withindist, within, contains, intersects, overlaps, nearest",
                      typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, predicate="nearest"):
        super(BroadcastSpatialJoin, self).__init__()
        self._java_obj = self._new_java_obj(self._fqdn, self.uid)

        # set params default values
        self._setDefault(predicate="nearest")

        # set params init values
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predicate="nearest"):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setPredicate(self, value):
        return self._set(predicate=value)

    def getPredicate(self):
        return self.getOrDefault(self.predicate)
