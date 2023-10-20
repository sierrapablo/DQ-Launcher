import unittest
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyquality.pyquality import Validator


class CheckDataLengthTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master(
            "local[1]").appName('CheckDataLenght Test').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_check_data_length(self):
        input_data = [('abc',), ('defg',), ('hi',), ('ijklmn',)]
        columns = ['column3']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator()

        # Definimos el resultado esperado para longitudes de 1 a 10
        expected_output = [
            [('abc', int(0)), ('defg', int(0)),
             ('hi', int(0)), ('ijklmn', int(0))],
            [('abc', int(0)), ('defg', int(0)),
             ('hi', int(1)), ('ijklmn', int(0))],
            [('abc', int(1)), ('defg', int(0)),
             ('hi', int(0)), ('ijklmn', int(0))],
            [('abc', int(0)), ('defg', int(1)),
             ('hi', int(0)), ('ijklmn', int(0))],
            [('abc', int(0)), ('defg', int(0)),
             ('hi', int(0)), ('ijklmn', int(1))]
        ]

        for length in range(1, 5):
            expected_df = self.spark.createDataFrame(
                expected_output[length-1], schema=['column3', 'column3_LENGTH_VALID']).withColumn(
                    'column3_LENGTH_VALID', col('column3_LENGTH_VALID').cast(IntegerType()))

            result_df = validator_test.check_data_length(df, 'column3', length)
            assertDataFrameEqual(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()


if __name__ == '__main__':
    unittest.main()
