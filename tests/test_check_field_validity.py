import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from dqvalidator.dqvalidator import Validator


class CheckFieldValidityTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("CheckFieldValidityTest") \
            .getOrCreate()
        self.validator_test = Validator()

    def tearDown(self):
        self.spark.stop()

    def test_check_field_validity_valid_true(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        columns = ['column1']
        df = self.spark.createDataFrame(input_data, schema=columns)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)

        result_df = self.validator_test.check_field_validity(dataframe=df,
                                                             column='column1',
                                                             ref_df=ref_df,
                                                             field_ref='column_ref',
                                                             valid=True)

        result_data = result_df.collect()
        expected_data = [('abc', 1), ('def', 0), ('ghi', 1), ('jkl', 0)]

        self.assertEqual(result_data, expected_data)

    def test_check_field_validity_valid_false(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        schema = StructType([StructField('column1', StringType(), True)])
        df = self.spark.createDataFrame(input_data, schema=schema)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)

        result_df = self.validator_test.check_field_validity(dataframe=df,
                                                             column='column1',
                                                             ref_df=ref_df,
                                                             field_ref='column_ref',
                                                             valid=False)

        result_data = [row.column1 for row in result_df.collect()]
        expected_data = ['NOT VALID', 'def', 'NOT VALID', 'jkl']

        self.assertEqual(result_data, expected_data)

    def test_check_field_validity_valid_undefined(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        columns = ['column1']
        df = self.spark.createDataFrame(input_data, schema=columns)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)

        result_df = self.validator_test.check_field_validity(dataframe=df,
                                                             column='column1',
                                                             ref_df=ref_df,
                                                             field_ref='column_ref')

        result_data = result_df.collect()
        expected_data = [('abc', 1), ('def', 0), ('ghi', 1), ('jkl', 0)]

        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
