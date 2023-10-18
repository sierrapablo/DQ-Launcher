"""
QualityValidations - v2.0

Autor: Pablo Sierra Lorente
Fecha: Octubre 2023
"""


import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.window import Window
import pandas as pd
import traceback

from launcher_errors import DataLoadingError


class Validator:

    def __init__(self,
                 spark: SparkSession,
                 logger: logging.Logger):
        """
        Constructor de la clase.

        Args:
            spark (SparkSession): Objeto SparkSession 
                utilizado para interactuar con Spark.
            logger (logging.Logger): Objeto Logger para registrar mensajes y eventos.

        Returns:
            None
        """
        self.spark = spark
        self.logger = logger

    def check_informed_fields(self, dataframe: DataFrame, column: str) -> DataFrame:
        """
        Verifica si cada campo de una columna está informado, y añade
        una nueva columna con 1 cuando lo está, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida.
        """
        self.logger.info(f"Validation for COMPLETENESS on: {column}")
        dataframe = dataframe.withColumn(
            column + "_INFORMED",
            when(col(column).isNotNull(), 1).otherwise(0))
        return dataframe

    def check_unique_fields(self, dataframe: DataFrame, column: str) -> DataFrame:
        """
        Verifica si cada campo de una columna es único, y añade
        una nueva columna con 1 cuando lo es, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            df_unique_flag (DataFrame): DataFrame con la columna añadida.
        """
        self.logger.info(f"Validation for UNIQUENESS on: {column}")
        window_spec = Window().partitionBy(column)
        df_with_counts = dataframe.withColumn(
            "COUNT", count(column).over(window_spec))
        df_unique_flag = df_with_counts.withColumn(
            column+'_UNIQUE',
            when(col("COUNT") == 1, 1).otherwise(0)
        ).drop("COUNT")
        return df_unique_flag

    def check_field_validity(self,
                             dataframe: DataFrame,
                             column: str,
                             table_ref: str,
                             field_ref: str) -> DataFrame:
        """
        Verifica si cada campo de una columna se encuentra recogido en una tabla de referencia.
        Añade una nueva columna con 1 cuando coincide, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.
            table_ref (str): string de la ubicación de la tabla de referencia.
                             Actualmente admite tablas en .xlsx
            field_ref (str): nombre de la columna en la tabla de referencia que se va a comparar.
            spark (SparkSession): Sesión de Spark con la que se opera.
            logger (logging.Logger): Objeto de regstro tipo logger.

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida.

        """
        try:
            ref_pd = pd.read_excel(table_ref)
            ref_df = self.spark.createDataFrame(ref_pd)
        except FileNotFoundError as e:
            self.logger.exception('File not found: ' + table_ref)
            self.logger.exception(
                'Error while searching for ' + table_ref + '. ' + str(e)
            )
            self.logger.exception('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))
        except Exception as e:
            self.logger.exception(
                'Error occurred while loading data: ' + str(e)
            )
            self.logger.exception('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))

        flag_col_name = column+'_VALIDO'
        dataframe = dataframe.withColumn(flag_col_name, lit(0))
        for row in dataframe.rdd.collect():
            column_value = row[column]
            if ref_df.filter(col(field_ref) == column_value).count() > 0:
                dataframe = dataframe.withColumn(flag_col_name, when(
                    col(column) == column_value, 1).otherwise(col(flag_col_name)))

        return dataframe

    def validar_formato_y_patrones(self, datos):
        # Implementar lógica para validar formato y patrones
        pass

    # Transformación y Estandarización de Datos
    def estandarizar_nombres_apellidos(self, datos):
        # Implementar lógica para estandarizar nombres y apellidos
        pass

    def estandarizar_tipos_documento(self, datos):
        # Implementar lógica para estandarizar tipos de documento
        pass

    # Validación de Longitudes y Tipos de Datos
    def validar_longitudes(self, datos):
        # Implementar lógica para validar longitudes
        pass

    def validar_tipos_datos(self, datos):
        # Implementar lógica para validar tipos de datos
        pass

    # Otros
    def generar_flags_sexo(self, datos):
        # Implementar lógica para generar flags de sexo
        pass