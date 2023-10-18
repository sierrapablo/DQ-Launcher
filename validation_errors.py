"""
QualityValidation Errors

Excepciones personalizadas utilizadas en QualityValidations.

Clases:
- DataLoadingError: Excepción para errores relacionados con la carga de datos.
- ValidationError: Excepción para errores relacionados con validaciones de datos.
- ConfigurationError: Excepción para errores relacionados con problemas de configuración.
- DataFrameNotFoundError: Excepción para errores relacionados con problemas de  DataFrame.

Autor: Pablo Sierra Lorente
Fecha: Octubre 2023
"""


class DataLoadingError(Exception):
    """
    Excepción personalizada para errores relacionados con la carga de datos.
    Esta excepción debería ser lanzada cuando ocurran problemas durante la carga de datos.
    """
    pass


class ValidationError(Exception):
    """
    Excepción personalizada para errores relacionados con validaciones de datos.
    Esta excepción debería ser lanzada cuando se detecten problemas durante las validaciones de datos.
    """
    pass


class ConfigurationError(Exception):
    """
    Excepción personalizada para errores relacionados con problemas de configuración.
    Esta excepción debería ser lanzada cuando haya errores en la configuración del programa.
    """
    pass


class DataFrameNotFoundError(Exception):
    """
    Excepción personalizada para errores relacionados con problemas con los DataFrame.
    Esta excepción debería ser lanzada cuando no se encuentre un DataFrame o este no sea válido.
    """
    pass

class SparkSessionError(Exception):
    """
    Excepción personalizada para errores relacionados con problemas con la sesión de Spark.
    Esta excepción debería ser lanzada cuando haya errores con el manejo de la sesión de Spark.
    """
    pass
