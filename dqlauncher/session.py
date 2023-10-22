"""
DQLauncher version 1.0

Session

Author: Pablo Sierra Lorente
Year: 2023
"""

from pyspark.sql import SparkSession
from dqlauncher.validator import Validator


class DQLauncherSession:

    builder = SparkSession.builder

    def __init__(self, appName="DQ Launcher App", master="local[*]", spark_config=None):
        super(DQLauncherSession, self).__init__()
        if appName:
            self.builder.appName(appName)
        if master:
            self.builder.master(master)
        if spark_config:
            for key, value in spark_config.items():
                self.builder.config(key, value)
        self.spark = self.builder.getOrCreate()

    def createValidator(self, data, schema):
        spark_df = self.spark.createDataFrame(data, schema=schema)
        return Validator(spark_df)
