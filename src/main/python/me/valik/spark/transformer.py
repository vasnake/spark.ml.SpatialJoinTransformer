# -*- coding: utf-8 -*-

from pyspark import keyword_only
from pyspark.ml.common import inherit_doc
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer


@inherit_doc
class BroadcastSpatialJoin(JavaTransformer, JavaMLReadable, JavaMLWritable):
    predicate = Param(Params._dummy(), "predicate", "spatial op, one of: withindist, within, contains, intersects, overlaps, nearest")
    fqdn = "me.valik.spark.transformer.BroadcastSpatialJoin"

    @keyword_only
    def __init__(self, uid=None, predicate = "nearest"):
        super(BroadcastSpatialJoin, self).__init__()

        if uid:
            self._java_obj = self._new_java_obj(self.fqdn, uid)
        else:
            self._java_obj = self._new_java_obj(self.fqdn)

        # set params default values
        self._setDefault(predicate="nearest")
        # set params init values
        kwargs = {"predicate": predicate}
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predicate="nearest"):
        kwargs = {"predicate": predicate}
        return self._set(**kwargs)

    def setPredicate(self, value):
        return self._set(predicate=value)

    def getPredicate(self):
        return self.getOrDefault(self.predicate)
