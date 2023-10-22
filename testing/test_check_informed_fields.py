import unittest
from dqlauncher.session import DQLauncherSession
from dqlauncher.utilities.errors import ValidationError


class TestCheckInformedFields(unittest.TestCase):
    """
    Unit tests for 'check_informed_fields' method in the Validator class.

    This test suite evaluates the behavior of the 'check_informed_fields' method
    in the Validator class, which is responsible for validating if specific columns
    are informed and creating a new column indicating the validation result.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test check_informed_fields App'
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
        result_data = result_vd.collect()
        expected_data = self.launcher.createValidator(
            expected_output, expected_columns
        ).collect()
        self.assertEqual(result_data, expected_data)

    def test_checkinformed_default(self):
        """
        Test case to validate the default behavior of 'check_informed_fields' method.
        """
        result_vd = self.validator_test.check_informed_fields(self.columns[0])
        expected_output = [('Paula', '30', 1),
                           ('Marlene', None, 1),
                           (None, '67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre', 'edad', 'nombre_INFORMED']
        self.validate_result(result_vd, expected_output, expected_columns)

    def test_checkinformed_customcolumn(self):
        """
        Test case to validate the behavior of 'check_informed_fields' method
        with a custom column name.
        """
        result_vd = self.validator_test.check_informed_fields(
            self.columns[0], 'RESULT')
        expected_output = [('Paula', '30', 1),
                           ('Marlene', None, 1),
                           (None, '67', 0),
                           ('Juan', '30', 1)]
        expected_columns = ['nombre', 'edad', 'RESULT']
        self.validate_result(result_vd, expected_output, expected_columns)

    def test_checkinformed_notvalidinput(self):
        """
        Test case to validate that the 'check_informed_fields' method raises
        a ValidationError when attempting to validate a non-existent column.
        """
        with self.assertRaises(ValidationError):
            self.validator_test.check_informed_fields('telefono')


if __name__ == '__main__':
    unittest.main()
