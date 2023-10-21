"""
DQLauncher version 1.0

Session

Author: Pablo Sierra Lorente
Year: 2023
"""

from pyspark.sql import SparkSession
from dqlauncher.validator import Validator


class SparkDQLauncher(SparkSession):

    def __init__(self, appName="DQ Launcher App"):
        super().__init__(appName)

    def CreateValidator(self, data, columns):
        spark_df = self.createDataFrame(data, columns)
        validator = Validator(self, spark_df)
        return validator
