import unittest
from dqlauncher.session import DQLauncherSession


class TestStdName(unittest.TestCase):
    """
    Unit tests for 'std_name' method in the Validator class.

    This test suite evaluates the behavior of the 'std_name' method
    in the Validator class, which is responsible for standarizing a specific string
    column.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test std_name App'
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

    def test_stdname_default(self):
        """
        Test case to validate the default behavior of 'std_name' method.
        """
        result_vd = self.validator_test.std_name(self.input_columns[0])

        expected_output = [('PAULA', '30'),
                           ('MARLENE', None),
                           (None, '67'),
                           ('JUAN', '30')]
        expected_columns = ['nombre', 'edad']

        self.validate_result(result_vd, expected_output, expected_columns)


if __name__ == '__main__':
    unittest.main()
