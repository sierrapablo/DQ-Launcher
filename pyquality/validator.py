"""
PyQuality! version 0.1

Author: Pablo Sierra Lorente
Year: 2023
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, broadcast, length, regexp_replace, upper, trim
from pyspark.sql.window import Window
from pyspark.sql.types import DataType
import pandas as pd
from typing import Optional
from pyquality.utilities.errors import *


class Validator:

    def __init__(self, spark: Optional[SparkSession] = None,):
        """
        Constructor de la clase Validator.

        Args:
            spark (Optional[SparkSession]): Objeto SparkSession utilizado para
                interactuar con Spark. Si no se proporciona, se creará uno por defecto.

        Returns:
            None
        """
        if spark is None:
            app_name = 'Validator APP'
            try:
                spark = SparkSession.builder \
                    .appName(app_name) \
                    .getOrCreate()
            except Exception as e:
                raise SparkSessionError('Error with Spark Session: ' + str(e))
        else:
            self.spark = spark

    @staticmethod
    def check_informed_fields(dataframe: DataFrame, column: str) -> DataFrame:
        """
        Verifica si cada campo de una columna está informado, y añade
        una nueva columna con 1 cuando lo está, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida.
        """
        dataframe = dataframe.withColumn(
            column + '_INFORMED',
            when(col(column).isNotNull(), 1).otherwise(0))
        return dataframe

    @staticmethod
    def check_unique_fields(dataframe: DataFrame, column: str) -> DataFrame:
        """
        Verifica si cada campo de una columna es único, y añade
        una nueva columna con 1 cuando lo es, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            df_unique_flag (DataFrame): DataFrame con la columna añadida.
        """
        window_spec = Window().partitionBy(column)
        df_with_counts = dataframe.withColumn(
            'COUNT', count(column).over(window_spec))
        df_unique_flag = df_with_counts.withColumn(
            column + '_UNIQUE',
            when(col('COUNT') == 1, 1).otherwise(0)
        ).drop('COUNT')
        return df_unique_flag

    def check_field_validity(self,
                             dataframe: DataFrame,
                             column: str,
                             table_ref: str,
                             field_ref: str,
                             sheet_name: Optional[str] = None,
                             valid: Optional[bool] = True) -> DataFrame:
        """
        Verifica si cada campo de una columna se encuentra en una tabla de referencia (DataFrame de PySpark).
        Añade una nueva columna con 1 cuando coincide y 0 cuando no.
        Modifica los valores existentes en la columna especificada del DataFrame original si valid es False.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar o añadir.
            table_ref (str): Ubicación del archivo de referencia (.xlsx o .csv).
            field_ref (str): Nombre de la columna en la tabla de referencia.
            sheet_name (Optional[str]): Nombre de la hoja en el archivo Excel
                (opcional, por defecto selecciona la primera hoja).
            valid (Optional[bool]): Indica si se debe añadir una nueva columna (True) o
                modificar los valores de la existente (False).

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida o los valores modificados.
        """
        try:
            if table_ref.endswith('.xlsx'):
                ref_pd = pd.read_excel(table_ref, sheet_name=sheet_name)
            elif table_ref.endswith('.csv'):
                ref_pd = pd.read_csv(table_ref)
            else:
                raise DataLoadingError('Unsupported file format.')
            ref_df = self.spark.createDataFrame(ref_pd)
        except FileNotFoundError as e:
            raise DataLoadingError('Data loading error: ' + str(e))
        except Exception as e:
            raise DataLoadingError('Data loading error: ' + str(e))

        join_type = 'left_outer' if valid else 'inner'

        joined_df = dataframe.join(
            broadcast(ref_df.select(field_ref)),
            col(column) == col(field_ref),
            join_type
        )
        if valid:
            flag_col_name = column + '_VALID'
            dataframe = joined_df.withColumn(
                flag_col_name, when(col(field_ref).isNotNull(), 1).otherwise(0)
            ).drop(field_ref)
        else:
            dataframe = joined_df.withColumn(
                column, when(col(field_ref).isNotNull(),
                             'NOT VALID').otherwise(col(column))
            ).drop(field_ref)
        return dataframe

    def check_data_length(self,
                          dataframe: DataFrame,
                          column: str,
                          data_length: int) -> DataFrame:
        """
        Verifica si cada campo de una columna coincide con la longitud especificada.
        Añade una columna con los valores: EQUAL si coincide, +-n si es más o menos largo.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.
            data_length (int): longitud del campo.

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida.
        """
        length_column = length(dataframe[column])
        dataframe = dataframe.withColumn(
            column + '_LENGTH_VALID',
            when(length_column == data_length, 'EQUAL')
            .when(length_column < data_length, '-' + str(data_length - length_column))
            .otherwise('+' + str(length_column - data_length))
        )
        return dataframe

    def check_data_type(self, dataframe: DataFrame, column: str, data_type: DataType) -> int:
        """
        Verifica si la columna dada en el dataframe es de tipo especificado.

        Args:
            dataframe (DataFrame): El dataframe que contiene la columna.
            column (str): Nombre de la columna que se verificará.
            data_type (DataType): Tipo de datos específico que se espera en la columna.

        Returns:
            int: 1 si la columna es del tipo concreto, 0 si no lo es.
        """
        column_data_type = dataframe.schema[column].dataType
        if column_data_type == data_type:
            return 1
        else:
            return 0

    def std_name(self,
                 dataframe: DataFrame,
                 column: str,
                 mode: Optional[str] = 'overwrite') -> DataFrame:
        """
        Estandariza los campos de la columna especificada siguiendo reglas
        de estandarización de nombres y apellidos. Maneja nombres y apellidos compuestos.

        Args:
            dataframe (DataFrame): El dataframe que contiene la columna.
            column (str): Nombre de la columna que se estandarizará.
            mode (str, optional): Modo de estandarización
                                  ('overwrite' por defecto, admite 'add').

        Returns:
            dataframe (DataFrame): DataFrame con la columna estandarizada.
        """
        if mode == 'add':
            dataframe = dataframe.withColumn(column + '_NO_STD', col(column))
        standardized_dataframe = dataframe.withColumn(
            column, upper(trim(regexp_replace(column, r'[^\w\s-]', ''))))
        standardized_dataframe = standardized_dataframe.withColumn(
            column, regexp_replace(column, r'-', ' '))
        return standardized_dataframe

    @staticmethod
    def get_null_count(dataframe: DataFrame, column: str) -> int:
        """
        Cuenta el número de valores nulos en una columna de un PySpark Dataframe.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            null_count (int): Número de valores nulos en la columna.
        """
        null_count = dataframe.where(col(column).isNull()).count()
        return int(null_count)

    def get_null_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calcula el porcentaje de valores nulos en una columnas del DataFrame.

        Args:
            dataframe (DataFrame): El DataFrame original.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            null_percentage_rounded (float): valor porcentual de nulos en la columna.
        """
        total_count = dataframe.count()
        null_count = self.get_null_count(dataframe, column)
        null_percentage = (null_count / total_count) * 100.0
        null_percentage_rounded = round(null_percentage, 2)
        return float(null_percentage_rounded)

    @staticmethod
    def get_informed_count(dataframe: DataFrame, column: str) -> int:
        """
        Cuenta el número de campos informados (no nulos) en una columna del DataFrame.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            informed_count (int): Número de campos informados en la columna.
        """
        informed_count = dataframe.where(col(column).isNotNull()).count()
        return int(informed_count)

    def get_informed_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calcula el porcentaje de campos informados en una columna del DataFrame.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            informed_percentage_rounded (float): valor (%) de informados en la columna.
        """
        total_count = dataframe.count()
        informed_count = self.get_informed_count(dataframe, column)
        informed_percentage = (informed_count / total_count) * 100.0
        informed_percentage_rounded = round(informed_percentage, 2)
        return float(informed_percentage_rounded)

    @staticmethod
    def get_unique_count(dataframe: DataFrame, column: str) -> int:
        """
        Cuenta el número de valores únicos en una columna del DataFrame.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            unique_count (int): Número de valores únicos en la columna.
        """
        unique_count = dataframe.select(column).distinct().count()
        return int(unique_count)

    def get_unique_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calcula el porcentaje de valores únicos en una columna del DataFrame.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.

        Returns:
            unique_percentage_rounded (float): valor (%) de únicos en la columna.
        """
        total_count = dataframe.count()
        unique_count = self.get_unique_count(dataframe, column)
        unique_percentage = (unique_count / total_count) * 100.0
        informed_percentage_rounded = round(unique_percentage, 2)
        return float(informed_percentage_rounded)

    def filter_df(dataframe: DataFrame, column: str, value: str) -> DataFrame:
        """
        Filtra un DataFrame en función de una columna y un valor específico.

        Args:
            df (DataFrame): El DataFrame de PySpark que se va a filtrar.
            column (str): El nombre de la columna en la que se aplicará el filtro.
            value (str): El valor que se utilizará para filtrar los datos en la columna.

        Returns:
            DataFrame: Un nuevo DataFrame que contiene solo las filas donde la columna
                especificada tiene el valor dado.
        """
        try:
            df_filtered = dataframe.filter(col(column) == value)
        except Exception as e:
            raise DataFrameError('Error with DataFrame: ' + str(e))
        return df_filtered
