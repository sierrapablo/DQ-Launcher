from pyquality.utilities.errors import *
from pyspark.sql import SparkSession

def close_spark_session(spark: SparkSession):
    """
    Cierra la sesión de Spark si está abierta.

    Raises:
        SparkSessionError: Si ocurre un error al cerrar la sesión de Spark.
    """
    if hasattr(spark, 'spark') and spark is not None:
        try:
            spark.stop()
        except Exception as e:
            spark_error_msg = 'Error occurred while closing Spark session: ' + \
                str(e)
            raise SparkSessionError(spark_error_msg)