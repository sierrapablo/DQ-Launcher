import unittest
from pyspark.sql import SparkSession
from dqlauncher.validator import Validator
from dqlauncher.utilities.errors import ValidationError


class TestCheckUniqueFields(unittest.TestCase):
    """
    Unit tests for the 'check_unique_fields' method in the Validator class.

    This test suite evaluates the behavior of the 'check_unique_fields' method
    in the Validator class, which is responsible for validating if values in specific
    columns are unique and creating a new column indicating the validation result.
    """

    @classmethod
    def setUpClass(cls):
        """
        Initializes a SparkSession before running the tests.
        """
        cls.spark = SparkSession.builder.master("local[1]").appName(
            'Test check_informed_fields').getOrCreate()

    def test_checkunique_default(self):
        """
        Test case to validate the default behavior of 'check_unique_fields' method.
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

        # Perform validation
        result_vd = validator_test.check_unique_fields(columns[1])
        expected_output = [('Paula', '30', 0),
                           ('Marlene', None, 0),
                           (None, '67', 1),
                           ('Juan', '30', 0)]
        expected_columns = ['nombre', 'edad', 'edad_UNIQUE']

        # Compare results
        result_data = result_vd.df.orderBy('nombre').collect()
        expected_data = self.spark.createDataFrame(
            expected_output, schema=expected_columns).orderBy('nombre').collect()
        self.assertEqual(result_data, expected_data)

    def test_checkunique_customcolumn(self):
        """
        Test case to validate the behavior of 'check_unique_fields' method
        with a custom result column name.
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

        # Perform validation with custom result column
        result_vd = validator_test.check_unique_fields(columns[1], 'RESULT')
        expected_output = [('Paula', '30', 0),
                           ('Marlene', None, 0),
                           (None, '67', 1),
                           ('Juan', '30', 0)]
        expected_columns = ['nombre', 'edad', 'RESULT']

        # Compare results
        result_data = result_vd.df.orderBy('nombre').collect()
        expected_data = self.spark.createDataFrame(
            expected_output, schema=expected_columns).orderBy('nombre').collect()
        self.assertEqual(result_data, expected_data)

    def test_checkunique_notvalidinput(self):
        """
        Test case to validate that the 'check_unique_fields' method raises
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
            validator_test.check_unique_fields('telefono')

        # Define expected error message
        expected_error_msg = "Column 'telefono' not found. Columns present: ['nombre', 'edad']"

        # Compare the raised exception message with the expected error message
        self.assertEqual(str(context.exception), expected_error_msg)


if __name__ == '__main__':
    unittest.main()
