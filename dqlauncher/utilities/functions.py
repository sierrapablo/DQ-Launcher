"""
Utility Functions

Functions used in PyQuality! versoin 0.1

close_spark_session: closes the provided Spark session if it is open.

Author: Pablo Sierra Lorente
Year: 2023
"""
from dqvalidator.utilities.errors import *
from pyspark.sql import SparkSession


def close_spark_session(spark: SparkSession):
    """
    Closes the Spark session if it is open.

    Args:
        spark (SparkSession): The SparkSession object to be closed.

    Raises:
        SparkSessionError: If an error occurs while closing the Spark session.
    """
    if spark is not None:
        try:
            spark.stop()
        except Exception as e:
            spark_error_msg = 'Error occurred while closing Spark session: ' + \
                str(e)
            raise SparkSessionError(spark_error_msg)
    else:
        raise SparkSessionError('No active Spark Session to close.')
