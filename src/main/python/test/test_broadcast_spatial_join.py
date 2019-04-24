# -*- coding: utf-8 -*-

import builtins
import pytest

from . import assert_frame_equal
from . import local_spark

from me.valik import BroadcastSpatialJoin

# TODO: add test for serializable pipeline

def check(transformer, input_df, expected_df, select=False):
    actual = transformer.transform(input_df)
    if select:
        actual = actual.select(expected_df.schema.names)

    assert_frame_equal(expected_df.toPandas(), actual.toPandas())

@pytest.fixture(scope="class")
def bag(request, local_spark):
    builtins.spark = local_spark

    poi = local_spark.createDataFrame(
        [("a", 1.1, 3.1), ("b", 2.1, 5.1)],
        ["poi_id", "lon", "lat"])
    poi.createOrReplaceTempView("poi")

    transformer = BroadcastSpatialJoin(
        dataset="poi", dataColumns="poi_id", datasetPoint="lon, lat", inputPoint="lon, lat"
    )

    return dict(transformer=transformer)


class TestBroadcastSpatialJoin(object):

    def test_simple_transform(self, bag):
        spark = builtins.spark

        input = spark.createDataFrame(
            [(0, 1.0, 3.0), (2, 2.0, 5.0)],
            ["id", "lon", "lat"])

        expected = spark.createDataFrame(
            [(0, 1.0, 3.0, "a"), (2, 2.0, 5.0, "b")],
            ["id", "lon", "lat", "poi_id"])

        check(bag["transformer"], input, expected)
