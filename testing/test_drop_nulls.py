import unittest
from dqlauncher.session import DQLauncherSession
from dqlauncher.utilities.errors import ValidationError


class TestDropNulls(unittest.TestCase):
    """
    Unit tests for 'drop_nulls' method in the Validator class.

    This test suite evaluates the behavior of the 'drop_nulls' method
    in the Validator class, which is responsible for dropping rows with null
    values in specific columns.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test drop_nulls App'
        master = 'local[1]'
        cls.launcher = DQLauncherSession(appName=app_name, master=master)

    @classmethod
    def tearDownClass(cls):
        cls.launcher.spark.stop()

    def setUp(self):
        self.input_data = [('Paula', '30'),
                           ('Marlene', None),
                           (None, '67'),
                           ('Juan', '30')]
        self.input_columns = ['nombre', 'edad']
        self.validator_test = self.launcher.createValidator(
            self.input_data, schema=self.input_columns)

    def validate_result(self, result_vd, expected_output, expected_columns):
        result_data = result_vd.collect()
        expected_data = self.launcher.createValidator(
            expected_output, expected_columns
        ).collect()
        self.assertEqual(result_data, expected_data)

    def test_dropnulls_default(self):
        """
        Test case to validate the default behavior of 'drop_nulls' method.
        """
        result_vd = self.validator_test.drop_nulls(
            self.input_columns[0])
        expected_output = [('Paula', '30'),
                           ('Marlene', None),
                           ('Juan', '30')]
        expected_columns = ['nombre', 'edad']
        self.validate_result(result_vd=result_vd,
                             expected_output=expected_output,
                             expected_columns=expected_columns)
        
    def test_dropnulls_double_input(self):
        """
        Test case to validate the behavior of 'drop_nulls' method when receiving
        two columns as an argument.
        """
        result_vd = self.validator_test.drop_nulls(
            self.input_columns[0], self.input_columns[1])
        expected_output = [('Paula', '30'),
                           ('Juan', '30')]
        expected_columns = ['nombre', 'edad']
        self.validate_result(result_vd=result_vd,
                             expected_output=expected_output,
                             expected_columns=expected_columns)


if __name__ == '__main__':
    unittest.main()
