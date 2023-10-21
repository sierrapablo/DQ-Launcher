import unittest
from pyspark.sql import SparkSession
from dqlauncher.validator import Validator
from dqlauncher.utilities.errors import ValidationError


class TestCheckInformedFields(unittest.TestCase):
    """
    Unit tests for the 'check_informed_fields' method in the Validator class.

    This test suite evaluates the behavior of the 'check_informed_fields' method
    in the Validator class, which is responsible for validating if specific columns
    are informed and creating a new column indicating the validation result.
    """

    @classmethod
    def setUpClass(cls):
        """
        Initializes a SparkSession before running the tests.
        """
        cls.spark = SparkSession.builder.master("local[1]").appName(
            'Test check_informed_fields').getOrCreate()

    def test_checkinformed_default(self):
        """
        Test case to validate the default behavior of 'check_informed_fields' method.
        """
        # Test data and columns setup
        input_data = [('Paula', '30'), ('Marlene', None),
                      (None, '67'), ('Juan', '30')]
        columns = ['nombre', 'edad']

        # Create DataFrame and Validator instance
        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        # Perform validation
        result_vd = validator_test.check_informed_fields(columns[0])
        expected_output = [('Paula', '30', 1),
                           ('Marlene', None, 1),
                           (None, '67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre', 'edad', 'nombre_INFORMED']

        # Compare results
        result_data = result_vd.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()
        self.assertEqual(result_data, expected_data)

    def test_checkinformed_customcolumn(self):
        """
        Test case to validate the behavior of 'check_informed_fields' method
        with a custom result column name.
        """
        # Test data and columns setup
        input_data = [('Paula', '30'), ('Marlene', None),
                      (None, '67'), ('Juan', '30')]
        columns = ['nombre', 'edad']

        # Create DataFrame and Validator instance
        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        # Perform validation with custom result column
        result_vd = validator_test.check_informed_fields(columns[0], 'RESULT')
        expected_output = [('Paula', '30', 1),
                           ('Marlene', None, 1),
                           (None, '67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre', 'edad', 'RESULT']

        # Compare results
        result_data = result_vd.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()
        self.assertEqual(result_data, expected_data)

    def test_checkinformed_notvalidinput(self):
        """
        Test case to validate that the 'check_informed_fields' method raises
        a ValidationError when attempting to validate a non-existent column.
        """
        # Test data and columns setup
        input_data = [('Paula', '30'),
                      ('Marlene', None),
                      (None, '67'),
                      ('Juan', '30')]
        columns = ['nombre', 'edad']

        # Create DataFrame and Validator instance
        df = self.spark.createDataFrame(input_data, schema=columns)
        validator_test = Validator(df)

        # Attempt validation on a non-existent column and validate the raised exception
        with self.assertRaises(ValidationError) as context:
            validator_test.check_informed_fields('telefono')

        # Define expected error message
        expected_error_msg = "Column 'telefono' not found. Columns present: ['nombre', 'edad']"

        # Compare the raised exception message with the expected error message
        self.assertEqual(str(context.exception), expected_error_msg)


if __name__ == '__main__':
    unittest.main()
