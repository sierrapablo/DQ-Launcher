import unittest
from pyspark.sql import SparkSession
from dqvalidator.dqvalidator import Validator


class TestCheckCompletenessUniquenessVd(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master(
            "local[1]").appName('Test Check VD').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_check_informed_fields(self):
        input_data = [(1,), (2,), (None,), (4,)]
        columns = ['column1']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator()

        result_df = validator_test.check_informed_fields(df, 'column1')
        expected_output = [(1, 1), (2, 1), (None, 0), (4, 1)]
        expected_columns = ['column1', 'column1_INFORMED']

        result_data = result_df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output, schema=expected_columns).collect()

        self.assertEqual(result_data, expected_data)

    def test_check_unique_fields(self):
        input_data = [(1,), (2,), (2,), (3,)]
        columns = ['column2']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator()

        result_df = validator_test.check_unique_fields(df, 'column2')
        expected_output = [(1, 1), (2, 0), (2, 0), (3, 1)]
        expected_columns = ['column2', 'column2_UNIQUE']

        result_data = result_df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output, schema=expected_columns).collect()

        self.assertEqual(result_data, expected_data)


if __name__ == '__main__':
    unittest.main()
