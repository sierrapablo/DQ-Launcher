"""
Validation Errors

Custom exceptions used in PyQuality! version 0.1

Classes:
- DataLoadingError: Exception for errors related to data loading.
- ValidationError: Exception for errors related to data validations.
- ConfigurationError: Exception for errors related to configuration issues.
- DataFrameNotFoundError: Exception for errors related to DataFrame problems.
- DataFrameError: Exception for errors related to DataFrame issues.
- SparkSessionError: Exception for errors related to Spark session problems.
- UnsupportedError: Exception for errors related to unsupported versions or modes.

Author: Pablo Sierra Lorente
Year: 2023
"""


class DataLoadingError(Exception):
    """
    Custom exception for errors related to data loading.
    This exception should be raised when problems occur during data loading.
    """
    pass


class ValidationError(Exception):
    """
    Custom exception for errors related to data validations.
    This exception should be raised when issues are detected during data validations.
    """
    pass


class ConfigurationError(Exception):
    """
    Custom exception for errors related to configuration problems.
    This exception should be raised when there are errors in the program's configuration.
    """
    pass


class DataFrameNotFoundError(Exception):
    """
    Custom exception for errors related to DataFrame problems.
    This exception should be raised when a DataFrame is not found or is not valid.
    """
    pass


class DataFrameError(Exception):
    """
    Custom exception for errors related to DataFrame problems.
    This exception should be raised when there is an issue with the DataFrame.
    """
    pass


class SparkSessionError(Exception):
    """
    Custom exception for errors related to Spark session problems.
    This exception should be raised when there are errors in managing the Spark session.
    """
    pass


class UnsupportedError(Exception):
    """
    Custom exception for errors related to unsupported versions and modes.
    This should be raised when problems with unsupported versions and modes occur.
    """
    pass
