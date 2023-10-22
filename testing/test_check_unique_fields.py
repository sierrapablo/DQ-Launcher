import unittest
from dqlauncher.session import DQLauncherSession
from dqlauncher.utilities.errors import ValidationError


class TestCheckUniqueFields(unittest.TestCase):
    """
    Unit tests for 'check_unique_fields' method in the Validator class.

    This test suite evaluates the behavior of the 'check_unique_fields' method
    in the Validator class, which is responsible for validating if rows in a column
    have duplicates, and creating a new column indicating the validation result.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test check_unique_fields App'
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
        self.columns = ['nombre', 'edad']
        self.validator_test = self.launcher.createValidator(
            self.input_data, schema=self.columns)
        
    def validate_result(self, result_vd, expected_output, expected_columns):
        result_data = result_vd.orderBy('nombre').collect()
        expected_data = self.launcher.createValidator(
            expected_output, expected_columns
        ).orderBy('nombre').collect()
        self.assertEqual(result_data, expected_data)

    def test_checkunique_default(self):
        """
        Test case to validate the default behavior of 'check_unique_fields' method.
        """
        result_vd = self.validator_test.check_unique_fields(self.columns[1])
        expected_output = [('Paula', '30', 0),
                           ('Marlene', None, 0),
                           (None, '67', 1),
                           ('Juan', '30', 0)]
        expected_columns = ['nombre', 'edad', 'edad_UNIQUE']
        self.validate_result(result_vd, expected_output, expected_columns)

    def test_checkunique_customcolumn(self):
        """
        Test case to validate the behavior of 'check_unique_fields' method when a
        custom column name is received.
        """
        result_vd = self.validator_test.check_unique_fields(self.columns[1], 'RESULT')
        expected_output = [('Paula', '30', 0),
                           ('Marlene', None, 0),
                           (None, '67', 1),
                           ('Juan', '30', 0)]
        expected_columns = ['nombre', 'edad', 'RESULT']
        self.validate_result(result_vd, expected_output, expected_columns)

    def test_checkunique_notvalidinput(self):
        """
        Test case to validate that the 'check_unique_fields' method raises
        a ValidationError when attempting to validate a non-existent column.
        """
        with self.assertRaises(ValidationError):
            self.validator_test.check_unique_fields('telefono')


if __name__ == '__main__':
    unittest.main()