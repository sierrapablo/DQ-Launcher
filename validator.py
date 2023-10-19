"""
QualityValidations - v2.0

Autor: Pablo Sierra Lorente
Fecha: Octubre 2023
"""


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.window import Window
import pandas as pd
import traceback
from typing import Optional
from validation_errors import *


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
            spark = self.create_spark_session()
        else:
            self.spark = spark

    @staticmethod
    def create_spark_session(app_name: str = "Validator APP") -> SparkSession:
        """
        Crea una sesión de Spark y la devuelve.

        Args:
            app_name (str): Nombre de la aplicación Spark. Por defecto, "Validator APP".

        Returns:
            SparkSession: Objeto SparkSession creado y configurado para la aplicación.

        Raises:
            SparkSessionError: Si ocurre un error al crear la sesión de Spark.
        """
        try:
            print('Creating Spark Session')
            spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()
            print('Spark Session successfully created: ' + app_name)
        except Exception as e:
            spark_error_msg = 'Error occurred while creating Spark session: ' + \
                str(e)
            print(spark_error_msg)
            print('Traceback: %s', traceback.format_exc())
            raise SparkSessionError('Error with Spark Session: ' + str(e))
        return spark

    def close_spark_session(self):
        """
        Cierra la sesión de Spark si está abierta.

        Raises:
            SparkSessionError: Si ocurre un error al cerrar la sesión de Spark.
        """
        if hasattr(self, 'spark') and self.spark is not None:
            try:
                print('Closing Spark Session')
                self.spark.stop()
                print('Spark Session closed successfully')
            except Exception as e:
                spark_error_msg = 'Error occurred while closing Spark session: ' + \
                    str(e)
                print(spark_error_msg)
                print('Traceback: %s', traceback.format_exc())
                raise SparkSessionError(spark_error_msg)

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
        print(f"Validation for COMPLETENESS on: {column}")
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
        print(f"Validation for UNIQUENESS on: {column}")
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
        Verifica si cada campo de una columna se encuentra en una tabla de referencia.
        Añade una nueva columna con 1 cuando coincide, y 0 cuando no.

        Args:
            dataframe (DataFrame): DataFrame de PySpark.
            column (str): Nombre de la columna que se va a validar.
            table_ref (str): string de la ubicación de la tabla de referencia.
                Actualmente admite tablas en .xlsx
            field_ref (str): nombre de la columna en la tabla de referencia.
            spark (SparkSession): Sesión de Spark con la que se opera.
            logger (logging.Logger): Objeto de regstro tipo logger.

        Returns:
            dataframe (DataFrame): DataFrame con la columna añadida.

        """
        try:
            ref_pd = pd.read_excel(table_ref)
            ref_df = self.spark.createDataFrame(ref_pd)
        except FileNotFoundError as e:
            print('File not found: ' + table_ref)
            print(
                'Error while searching for ' + table_ref + '. ' + str(e)
            )
            print('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))
        except Exception as e:
            print(
                'Error occurred while loading data: ' + str(e)
            )
            print('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))

        flag_col_name = column+'_VALID'
        dataframe = dataframe.withColumn(flag_col_name, lit(0))
        for row in dataframe.rdd.collect():
            column_value = row[column]
            if ref_df.filter(col(field_ref) == column_value).count() > 0:
                dataframe = dataframe.withColumn(flag_col_name, when(
                    col(column) == column_value, 1).otherwise(col(flag_col_name)))

        return dataframe

    def check_data_length(self,
                          dataframe: DataFrame,
                          column: str,
                          data_lenght: int) -> DataFrame:
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

        pass

    def check_data_type(self, dataframe: DataFrame, column: str, data_type: str) -> int:
        """
        Verifica si la columna dada en el dataframe es de tipo especificado.

        Args:
            dataframe (DataFrame): El dataframe que contiene la columna.
            columna (str): Nombre de la columna que se verificará.

        Returns:
            int: 1 si la columna es del tipo concreto, 0 si no lo es.
        """

        pass

    # Transformación y Estandarización de Datos
    def std_name(self,
                 dataframe: DataFrame,
                 column: str,
                 mode: Optional[str] = "overwrite") -> DataFrame:
        """
        Estandariza los campos de la columna especificada siguiendo reglas
        de estandarización de nombres y apellidos. Maneja nombres y apellidos compuestos.

        Args:
            dataframe (DataFrame): El dataframe que contiene la columna.
            column (str): Nombre de la columna que se estandarizará.
            mode (str, optional): Modo de estandarización
                                  ("overwrite" por defecto, admite "add").

        Returns:
            dataframe (DataFrame): DataFrame con la columna estandarizada.
        """
        pass

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

    def load_data_from_excel(self,
                             file_path: str,
                             sheet_name: str) -> DataFrame:
        """
        Carga datos desde un archivo Excel a un DataFrame de Spark.

        Args:
            file_path (str): Ruta del archivo Excel.
            sheet_name (str): Nombre de la hoja en el archivo Excel.

        Returns:
            DataFrame: DataFrame de Spark que contiene los datos del archivo Excel.

        Raises:
            Exception: Si ocurre un error al cargar los datos desde el archivo Excel.
        """
        try:
            print('Loading data from: ' + file_path)
            data_pd = pd.read_excel(file_path, sheet_name=sheet_name)
            data_df = self.spark.createDataFrame(data_pd)
            print('Data successfully read from: ' + file_path)
            return data_df
        except FileNotFoundError as e:
            print('File not found: ' + file_path)
            print(
                'Error while searching for ' + file_path + '. ' + str(e)
            )
            print('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))
        except Exception as e:
            print(
                'Error occurred while loading data: ' + str(e)
            )
            print('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))

    def save_to_postgres(self,
                         df: DataFrame,
                         connection_url: str,
                         table_name: str,
                         connection_properties: dict,
                         writing_mode: str):
        """
        Guarda un DataFrame de Spark en una tabla PostgreSQL con una conexión JDBC.

        Args:
            results_df (DataFrame): DataFrame de Spark que se va a guardar en la BD.
            connection_url (str): URL de conexión a la base de datos PostgreSQL.
            table_name (str): Nombre de la tabla en la que se van a guardar los datos.
            connection_properties (dict): Propiedades de la conexión a la BD PostgreSQL.
            writing_mode (str): Modo de escritura para la tabla:
                ("overwrite", "append", "ignore", "error").

        Raises:
            Exception: Si ocurre un error al guardar los datos en la BD PostgreSQL.
        """
        try:
            df.write.jdbc(
                url=connection_url,
                table=table_name,
                mode=writing_mode,
                properties=connection_properties
            )
            print(
                'Table correctly updated in the Database: ' + table_name)
        except Exception as e:
            print(
                f'Error occurred while saving data in {table_name}: {str(e)}'
            )
            print('Traceback: %s', traceback.format_exc())
            raise DataLoadingError('Data loading error: ' + str(e))

    def filter_df(self, df: DataFrame, column: str, value: str) -> DataFrame:
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
        print('Filtering data from ' + column + '=' + value)

        try:
            df_filtered = df.filter(col(column) == value)
        except Exception as e:
            print('Traceback: %s', traceback.format_exc())
            raise DataFrameError('Error with DataFrame: ' + str(e))

        return df_filtered
