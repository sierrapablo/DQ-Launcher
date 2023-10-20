import unittest
from pyspark.sql import SparkSession
from dqvalidator.dqvalidator import Validator


class CheckFieldValidityTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("CheckFieldValidityTest") \
            .getOrCreate()
        self.validator_test = Validator()

    def tearDown(self):
        self.spark.stop()

    def test_check_field_validity_valid_defined(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        columns = ['column1']
        df = self.spark.createDataFrame(input_data, schema=columns)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_df.write.csv('tests\\ref_check_field_validity.csv',
                         header=True, mode='overwrite')

        result_df = self.validator_test.check_field_validity(
            df, 'column1', 'tests\\ref_check_field_validity.csv', 'column_ref', valid=True)

        expected_data = [(1,), (0,), (1,), (0,)]
        expected_columns = ['column1_VALID']
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_columns)

        self.assertDataFrameEqual(result_df, expected_df)

    def test_check_field_validity_unvalid_defined(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        columns = ['column1']
        df = self.spark.createDataFrame(input_data, schema=columns)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_df.write.csv('tests\\ref_check_field_validity.csv',
                         header=True, mode='overwrite')

        result_df = self.validator_test.check_field_validity(
            df, 'column1', 'tests\\ref_check_field_validity.csv', 'column_ref', valid=False)

        expected_data = [('abc',), ('NOT VALID',), ('ghi',), ('NOT VALID',)]
        expected_columns = ['column1']
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_columns)

        self.assertDataFrameEqual(result_df, expected_df)

    def test_check_field_validity_valid_undefined(self):

        input_data = [('abc',), ('def',), ('ghi',), ('jkl',)]
        columns = ['column1']
        df = self.spark.createDataFrame(input_data, schema=columns)

        ref_data = [('xyz',), ('ghi',), ('abc',)]
        ref_columns = ['column_ref']
        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_df.write.csv('tests\\ref_check_field_validity.csv',
                         header=True, mode='overwrite')

        result_df = self.validator_test.check_field_validity(
            df, 'column1', 'tests\\ref_check_field_validity.csv', 'column_ref')

        expected_data = [(1,), (0,), (1,), (0,)]
        expected_columns = ['column1_VALID']
        expected_df = self.spark.createDataFrame(
            expected_data, schema=expected_columns)

        self.assertDataFrameEqual(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()
