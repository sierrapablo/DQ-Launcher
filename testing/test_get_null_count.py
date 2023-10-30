import unittest
from dqlauncher.session import DQLauncherSession
from dqlauncher.utilities.errors import ValidationError


class TestGetNullCount(unittest.TestCase):
    """
    Unit tests for 'get_null_count' method in the Validator class.

    This test suite evaluates the behavior of the 'get_null_count' method
    in the Validator class, which is responsible for counting how many null values
    are in a specific column.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test get_null_count App'
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

    def test_getnull_default(self):
        """
        Test case to validate the default behavior of 'get_null_count' method.
        """
        result = self.validator_test.get_null_count(self.columns[0])
        expected_output = 1
        self.assertEqual(result, expected_output)

    def test_getnull_notvalidinput(self):
        """
        Test case to validate that the 'get_null_count' method raises
        a ValidationError when attempting to validate a non-existent column.
        """
        with self.assertRaises(ValidationError):
            self.validator_test.get_null_count('telefono')


if __name__ == '__main__':
    unittest.main()