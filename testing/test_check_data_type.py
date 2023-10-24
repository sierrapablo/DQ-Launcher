import unittest
from dqlauncher.session import DQLauncherSession
from pyspark.sql.types import StringType, IntegerType


class TestCheckDataType(unittest.TestCase):
    """
    Unit tests for 'check_data_type' method in the Validator class.

    This test suite evaluates the behavior of the 'check_data_type' method
    in the Validator class, which is responsible for validating if a specific column
    mathes the type from provided.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test check_data_type App'
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

    def validate_result(self, result, expected_output):
        self.assertEqual(result, expected_output)

    def test_checktype_default(self):
        """
        Test case to validate the default behavior of 'check_data_type' method.
        """
        ok_result = self.validator_test.check_data_type(
            self.input_columns[0], StringType)
        expected_ok_output = int(1)
        self.validate_result(
            result=ok_result, expected_output=expected_ok_output)
        ko_result = self.validator_test.check_data_type(
            self.input_columns[0], IntegerType)
        expected_ko_output = int(0)
        self.validate_result(
            result=ko_result, expected_output=expected_ko_output)


if __name__ == '__main__':
    unittest.main()
