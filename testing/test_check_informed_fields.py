import unittest
from pyspark.sql import SparkSession
from dqlauncher.validator import Validator
from dqlauncher.utilities.errors import ValidatorError


class TestCheckInformedFields(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master(
            "local[2]").appName('Test Check Informded & Unique fields').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_checkinformed_default(self):
        input_data = [('Paula','30'), ('Marlene', None), (None,'67'), ('Juan', '30')]
        columns = ['nombre', 'edad']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        result_vd = validator_test.check_informed_fields(columns[0])
        expected_output = [('Paula','30', 1),
                           ('Marlene', None, 1),
                           (None,'67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre','edad', 'nombre_INFORMED']

        result_data = result_vd.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
            ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkinformed_customcolumn(self):
        input_data = [('Paula','30'), ('Marlene', None), (None,'67'), ('Juan', '30')]
        columns = ['nombre', 'edad']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        result_vd = validator_test.check_informed_fields(columns[0], 'RESULT')
        expected_output = [('Paula','30', 1),
                           ('Marlene', None, 1),
                           (None,'67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre','edad', 'RESULT']

        result_data = result_vd.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
            ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkinformed_notvalidinput(self):
        input_data = [('Paula','30'), ('Marlene', None), (None,'67'), ('Juan', '30')]
        columns = ['nombre', 'edad']

        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        with self.assertRaises(ValidatorError):
            validator_test.check_informed_fields(columns[0], 'DNI')

    
if __name__ == '__main__':
    unittest.main()
