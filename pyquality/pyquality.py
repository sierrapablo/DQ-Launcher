"""
PyQuality! version 0.1

Author: Pablo Sierra Lorente
Year: 2023
"""

import pandas
from pyquality.utilities.errors import *
from pyquality.utilities.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import DataType
from pyspark.sql.window import Window
from typing import Optional


class PyQuality:

    def __init__(self, spark: Optional[SparkSession] = None,):
        """
        Constructor of Validator.

        Args:
            spark (Optional[SparkSession]): SparkSession object used for
                interacting with Spark. If not provided, a default one will be created.

        Returns:
            None
        """
        if spark is None:
            app_name = 'PyQuality Spark APP'
            try:
                spark = SparkSession.builder \
                    .appName(app_name) \
                    .getOrCreate()
            except Exception as e:
                raise SparkSessionError('Error with Spark Session: ' + str(e))
        else:
            self.spark = spark

    def close(self):
        if hasattr(self, 'spark') and self.spark is not None:
            close_spark_session(self.spark)
        else:
            raise SparkSessionError('No active Spark Session to close.')

    @staticmethod
    def check_informed_fields(dataframe: DataFrame, column: str) -> DataFrame:
        """
        Checks if each field in a column is informed and adds
        a new column with 1 when it is, and 0 when it is not.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to validate.

        Returns:
            dataframe (DataFrame): DataFrame with the added column.
        """
        dataframe = dataframe.withColumn(
            column + '_INFORMED',
            when(col(column).isNotNull(), 1).otherwise(0))
        return dataframe

    @staticmethod
    def check_unique_fields(dataframe: DataFrame, column: str) -> DataFrame:
        """
        Checks if each field in a column is unique and adds
        a new column with 1 when it is, and 0 when it is not.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to validate.

        Returns:
            df_unique_flag (DataFrame): DataFrame with the added column.
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
        Checks if each field in a column is in a reference table (PySpark DataFrame).
        Adds a new column with 1 when it matches and 0 when it doesn't.
        Modifies existing values in the specified column of the original DataFrame if valid is False.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to validate or add.
            table_ref (str): Location of the reference file (.xlsx or .csv).
            field_ref (str): Name of the column in the reference table.
            sheet_name (Optional[str]): Name of the sheet in the Excel file
                (optional, defaults to the first sheet).
            valid (Optional[bool]): Indicates whether to add a new column (True) or
                modify the values of the existing one (False).

        Returns:
            dataframe (DataFrame): DataFrame with the added column or modified values.
        """
        try:
            if table_ref.endswith('.xlsx'):
                ref_pd = pandas.read_excel(table_ref, sheet_name=sheet_name)
            elif table_ref.endswith('.csv'):
                ref_pd = pandas.read_csv(table_ref)
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
        Checks if each field in a column matches the specified length.
        Adds a column with values: EQUAL if it matches, +-n if it is shorter or longer.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to validate.
            data_length (int): Length of the field.

        Returns:
            dataframe (DataFrame): DataFrame with the added column.
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
        Checks if the given column in the dataframe is of the specified type.

        Args:
            dataframe (DataFrame): The dataframe containing the column.
            column (str): Name of the column to be checked.
            data_type (DataType): Specific data type expected in the column.

        Returns:
            int: 1 if the column is of the specific type, 0 if it is not.
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
        Standardizes fields in the specified column following
        rules for standardization of names and surnames. Handles compound names and surnames.

        Args:
            dataframe (DataFrame): The dataframe containing the column.
            column (str): Name of the column to be standardized.
            mode (str, optional): Standardization mode
                                  ('overwrite' by default, also accepts 'add').

        Returns:
            dataframe (DataFrame): DataFrame with the standardized column.
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
        Counts the number of null values in a column of a PySpark DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            null_count (int): Number of null values in the column.
        """
        null_count = dataframe.where(col(column).isNull()).count()
        return int(null_count)

    def get_null_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calculates the percentage of null values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): The original DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            null_percentage_rounded (float): Percentage value of nulls in the column.
        """
        total_count = dataframe.count()
        null_count = self.get_null_count(dataframe, column)
        null_percentage = (null_count / total_count) * 100.0
        null_percentage_rounded = round(null_percentage, 2)
        return float(null_percentage_rounded)

    @staticmethod
    def get_informed_count(dataframe: DataFrame, column: str) -> int:
        """
        Counts the number of informed fields (non-null) in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            informed_count (int): Number of informed fields in the column.
        """
        informed_count = dataframe.where(col(column).isNotNull()).count()
        return int(informed_count)

    def get_informed_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calculates the percentage of informed fields in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            informed_percentage_rounded (float): Percentage value of informed fields in the column.
        """
        total_count = dataframe.count()
        informed_count = self.get_informed_count(dataframe, column)
        informed_percentage = (informed_count / total_count) * 100.0
        informed_percentage_rounded = round(informed_percentage, 2)
        return float(informed_percentage_rounded)

    @staticmethod
    def get_unique_count(dataframe: DataFrame, column: str) -> int:
        """
        Counts the number of unique values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            unique_count (int): Number of unique values in the column.
        """
        unique_count = dataframe.select(column).distinct().count()
        return int(unique_count)

    def get_unique_percentage(self, dataframe: DataFrame, column: str) -> float:
        """
        Calculates the percentage of unique values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            unique_percentage_rounded (float): Percentage value of unique values in the column.
        """
        total_count = dataframe.count()
        unique_count = self.get_unique_count(dataframe, column)
        unique_percentage = (unique_count / total_count) * 100.0
        informed_percentage_rounded = round(unique_percentage, 2)
        return float(informed_percentage_rounded)

    def filter_df(dataframe: DataFrame, column: str, value: str) -> DataFrame:
        """
        Filters a DataFrame based on a specific column and value.

        Args:
            dataframe (DataFrame): The PySpark DataFrame to be filtered.
            column (str): The name of the column on which the filter will be applied.
            value (str): The value that will be used to filter the data in the column.

        Returns:
            DataFrame: A new DataFrame containing only the rows where the specified column
                has the given value.
        """
        try:
            df_filtered = dataframe.filter(col(column) == value)
        except Exception as e:
            raise DataFrameError('Error with DataFrame: ' + str(e))
        return df_filtered
