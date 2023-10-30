"""
DQLauncher version 1.0

Validator

Author: Pablo Sierra Lorente
Year: 2023
"""

from dqlauncher.utilities.errors import *
from dqlauncher.utilities.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import DataType
from pyspark.sql.window import Window
from typing import Optional, Union
from pyspark.sql import DataFrame


class Validator(DataFrame):

    def __init__(self, df):
        super(Validator, self).__init__(df._jdf, df.sql_ctx)

    def check_informed_fields(self,
                              column: str,
                              result_column: Optional[str] = None) -> "Validator":
        """
        Checks if each field in a column is informed and adds
        a new column with 1 when it is, and 0 when if it is not.

        Args:
            column (str): Name of the column to validate.
            result_column (Optional[str]): name of the resulted column.
                If not provided, default column + '_INFORMED'

        Returns:
            validator (SparkValidator): Validator with the added column.
        """
        if result_column is None:
            result_column = column + '_INFORMED'
        try:
            df = self.withColumn(
                result_column,
                when(col(column).isNotNull(), 1).otherwise(0))
            validator = Validator(df)
            return validator
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def check_unique_fields(self,
                            column: str,
                            result_column: Optional[str] = None) -> "Validator":
        """
        Checks if each field in a column is unique and adds
        a new column with 1 when it is, and 0 when it is not.

        Args:
            column (str): Name of the column to validate.
            result_column (Optional[str]): name of the resulted column.
                If not provided, default column + '_UNIQUE'

        Returns:
            validator (Validator): Validator with the added column.
        """
        if result_column is None:
            result_column = column + '_UNIQUE'
        try:
            window_spec = Window().partitionBy(column)
            df_with_counts = self.withColumn(
                'COUNT', count(column).over(window_spec))
            max_count = max('COUNT').over(window_spec)
            df_unique_flag = df_with_counts.withColumn(
                result_column,
                when((col(column).isNotNull()) &
                     (col('COUNT') == 1) &
                     (col('COUNT') == max_count), 1).otherwise(0)
            ).drop('COUNT')
            return Validator(df_unique_flag)
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def check_field_validity(self,
                             column: str,
                             ref_df: Union["Validator", "DataFrame"],
                             field_ref: str,
                             valid: Optional[bool] = True,
                             result_column: Optional[str] = None) -> "Validator":
        """
        Checks if each field in a column is in a reference table (Validator or DataFrame).
        Adds a new column with 1 when it matches and 0 when it doesn't.
        Modifies existing values in the specified column of the original Validator if valid is False.

        Args:
            column (str): Name of the column to validate or add.
            ref_df (Union[Validator, DataFrame]): Validator or DataFrame for the reference table.
            field_ref (str): Name of the column in the reference table.
            valid (Optional[bool]): Indicates whether to add a new column (True) or
                modify the values of the existing one (False).
            result_column (Optional[str]): name of the resulted column.
                If not provided, default column + '_VALID'

        Returns:
            validator (Validator): Validator with the added column or modified values.
        """
        try:
            joined_df = self.join(
                broadcast(ref_df.select(field_ref)),
                col(column) == col(field_ref),
                'left'
            )
            if valid:
                if result_column is None:
                    result_column = column + '_VALID'
                dataframe = joined_df.withColumn(
                    result_column, when(
                        col(field_ref).isNotNull(), 1).otherwise(0)
                ).drop(field_ref)
            else:
                dataframe = joined_df.withColumn(
                    column,
                    when(col(field_ref).isNotNull(),
                         'NOT VALID').otherwise(col(column))
                ).drop(field_ref)
            validator = Validator(dataframe)
            return validator
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def check_data_length(self,
                          column: str,
                          data_length: int,
                          result_column: Optional[str] = None) -> "Validator":
        """
        Checks if each field in a column matches the specified length.
        Adds a column with 1 if the length matches, and 0 otherwise.

        Args:
            column (str): Name of the column to validate.
            data_length (int): Length of the field.

        Returns:
            validator (Validator): Validator with the added column.
        """
        if result_column is None:
            result_column = column + '_LENGHT_CHECKED'
        try:
            length_column = expr(f'length({column})')
            match_length = when(length_column == data_length, 1).otherwise(0)
            dataframe = self.withColumn(
                result_column,
                match_length.cast('int')
            )
            validator = Validator(dataframe)
            return validator
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def check_data_type(self,
                        column: str,
                        data_type: DataType) -> int:
        """
        Checks if the given column in the dataframe is of the specified type.

        Args:
            dataframe (DataFrame): The dataframe containing the column.
            column (str): Name of the column to be checked.
            data_type (DataType): Specific data type expected in the column.

        Returns:
            int: 1 if the column is of the specific type, 0 if it is not.
        """
        try:
            column_data_type = self.schema[column].dataType
            if isinstance(column_data_type, data_type):
                return int(1)
            else:
                return int(0)
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def std_name(self,
                 column: str,
                 mode: Optional[str] = 'overwrite') -> "Validator":
        """
        Standardizes fields in the specified column following
        rules for standardization of names and surnames. Handles compound names and surnames.

        Args:
            column (str): Name of the column to be standardized.
            mode (str, optional): Standardization mode
                                  ('overwrite' by default, also accepts 'add').

        Returns:
            validator (Validator): Validator with the standardized column.
        """
        try:
            if mode == 'add':
                self.withColumn(column + '_NO_STD', col(column))
            standardized_df = self.withColumn(
                column, upper(trim(regexp_replace(column, r'[^\w\s-]', ''))))
            standardized_df = standardized_df.withColumn(
                column, regexp_replace(column, r'-', ' '))
            validator = Validator(standardized_df)
            return validator
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e

    def get_null_count(self, column: str) -> int:
        """
        Counts the number of null values in a column of a PySpark DataFrame.

        Args:
            column (str): Name of the column to be validated.

        Returns:
            null_count (int): Number of null values in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        null_count = self.where(col(column).isNull()).count()
        return int(null_count)

    def get_null_percentage(self, column: str) -> float:
        """
        Calculates the percentage of null values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): The original DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            null_percentage_rounded (float): Percentage value of nulls in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        total_count = self.count()
        null_count = self.get_null_count(column)
        null_percentage = (null_count / total_count) * 100.0
        null_percentage_rounded = round(null_percentage, 2)
        return float(null_percentage_rounded)

    def get_informed_count(self, column: str) -> int:
        """
        Counts the number of informed fields (non-null) in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            informed_count (int): Number of informed fields in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        informed_count = self.where(col(column).isNotNull()).count()
        return int(informed_count)

    def get_informed_percentage(self, column: str) -> float:
        """
        Calculates the percentage of informed fields in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            informed_percentage_rounded (float): Percentage value of informed fields in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        total_count = self.count()
        informed_count = self.get_informed_count(column)
        informed_percentage = (informed_count / total_count) * 100.0
        informed_percentage_rounded = round(informed_percentage, 2)
        return float(informed_percentage_rounded)

    def get_unique_count(self, column: str) -> int:
        """
        Counts the number of unique values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            unique_count (int): Number of unique values in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        unique_count = self.select(column).distinct().count()
        return int(unique_count)

    def get_unique_percentage(self, column: str) -> float:
        """
        Calculates the percentage of unique values in a column of the DataFrame.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            column (str): Name of the column to be validated.

        Returns:
            unique_percentage_rounded (float): Percentage value of unique values in the column.
        """
        if column not in self.columns:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg)
        total_count = self.count()
        unique_count = self.get_unique_count(column)
        unique_percentage = (unique_count / total_count) * 100.0
        informed_percentage_rounded = round(unique_percentage, 2)
        return float(informed_percentage_rounded)

    def filter_vd(self, column: str, value: str) -> "Validator":
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
            if column not in self.columns:
                error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
                raise ValidationError(error_msg)
            df_filtered = self.filter(col(column) == value)
        except Exception as e:
            raise ValidatorError('Error with Validator: ' + str(e))
        return df_filtered

    def drop_nulls(self, *columns: str) -> "Validator":
        """
        Drops rows in the DataFrame where specified columns do have null values.

        Args:
            *columns (str): Variable length argument list of column names to filter.

        Returns:
            Validator: A new Validator instance with the filtered columns.

        Raises:
            ValidationError: If any of the specified columns are not found in the Validator.
        """
        try:
            dataframe = self
            for column in columns:
                dataframe = dataframe.filter(col(column).isNotNull())
            return Validator(dataframe)
        except Exception as e:
            error_msg = f"Column '{column}' not found. Columns present: {self.columns}"
            raise ValidationError(error_msg) from e
        
    def drop_duplicates(self, *columns: str, mode: Optional[str]='concat') -> "Validator":
        """
        Drop duplicate rows based on specified columns in the Validator.

        Args:
            *columns (str): Variable length argument list of column names to identify duplicates.
            mode (str, optional): Mode of dropping duplicates. 'concat' concatenates columns and drops duplicates, 
                                'separate' drops duplicates in each specified column separately. Default is 'concat'.

        Returns:
            Validator: A new Validator instance with duplicates removed.

        Raises:
            ValueError: If an invalid mode is provided.
            ValidationError: If any of the specified columns are not found in the Validator.
        """
        try:
            if mode == 'concat':
                concatenated_columns = concat_ws("", *columns)
                dataframe = self \
                    .withColumn("concatenated",concatenated_columns) \
                    .dropDuplicates(subset=["concatenated"]) \
                    .drop("concatenated")
            elif mode == 'separate':
                dataframe = self.dropDuplicates(subset=columns)
            else:
                raise ValueError("Mode not valid. Use 'concat' or 'separate'.")
            return Validator(dataframe)
        except Exception as e:
            error_msg = f"Error trying to drop duplicates: {str(e)}"
            raise ValidationError(error_msg) from e