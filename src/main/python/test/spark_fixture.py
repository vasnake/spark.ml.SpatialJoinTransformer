# -*- coding: utf-8 -*-

import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark(request):
    spark_jars = os.environ['SPARK_JARS']
    print("SPARK_JARS: {}".format(spark_jars))

    spark = SparkSession.builder.master(
        "local[1]"
    ).config(
        key='spark.jars', value=spark_jars
    ).config(
        key='spark.driver.extraLibraryPath', value=spark_jars
    ).config(
        key='spark.executor.extraLibraryPath', value=spark_jars
    ).config(
        key='spark.checkpoint.dir', value='/tmp/checkpoints'
    ).getOrCreate()

    request.addfinalizer(lambda: spark.stop())
    return spark
