import unittest
from dqlauncher.session import DQLauncherSession
from dqlauncher.utilities.errors import ValidationError


class TestCheckFieldValidity(unittest.TestCase):
    """
    Unit tests for 'check_field_validity' method in the Validator class.

    This test suite evaluates the behavior of the 'check_field_validity' method
    in the Validator class, which is responsible for validating if specific columns
    contain values that appear in a reference dataset and creating a new column
    indicating the validation result.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test check_field_validity App'
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

        self.ref_data = [('Marlene',)]
        self.ref_columns = ['ref_nombre']
        self.validator_ref = self.launcher.createValidator(
            self.ref_data, schema=self.ref_columns)

    def validate_result(self, result_vd, expected_output, expected_columns):
        result_data = result_vd.collect()
        expected_data = self.launcher.createValidator(
            expected_output, expected_columns
        ).collect()
        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_default(self):
        """
        Test case to validate the default behavior of 'check_field_validity' method.
        """
        result_vd = self.validator_test.check_field_validity(self.input_columns[0],
                                                             self.validator_ref,
                                                             self.ref_columns[0])
        expected_output = [('Paula', '30', 0),
                           ('Marlene', None, 1),
                           (None, '67', 0),
                           ('Juan', '30', 0)]
        expected_columns = ['nombre', 'edad', 'ref_nombre_VALID']
        self.validate_result(result_vd=result_vd,
                             expected_output=expected_output,
                             expected_columns=expected_columns)


if __name__ == '__main__':
    unittest.main()
