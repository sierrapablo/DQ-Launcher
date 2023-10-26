import unittest
from dqlauncher.session import DQLauncherSession


class TestDropDuplicates(unittest.TestCase):
    """
    Unit tests for 'drop_duplicates' method in the Validator class.

    This test suite evaluates the behavior of the 'drop_duplicates' method
    in the Validator class, which is responsible for dropping rows with duplicates
    values in specific columns.
    """

    @classmethod
    def setUpClass(cls):
        app_name = 'Test drop_duplicates App'
        master = 'local[1]'
        cls.launcher = DQLauncherSession(appName=app_name, master=master)

    @classmethod
    def tearDownClass(cls):
        cls.launcher.spark.stop()

    def setUp(self):
        self.input_data = [('Paula', '30'),
                           ('Marlene', None),
                           ('Paula', '27'),
                           ('Juan', '30'),
                           (None, '67'),
                           ('Juan', '30')]
        self.input_columns = ['nombre', 'edad']
        self.validator_test = self.launcher.createValidator(
            self.input_data, schema=self.input_columns)

    def validate_result(self, result_vd, expected_output, expected_columns):
        def sort_key(row):
            return tuple(str(value) if value is not None else '\uffff' for value in row)
        result_data = sorted(result_vd.collect(), key=sort_key)
        expected_data = sorted(self.launcher.createValidator(
            expected_output, expected_columns).collect(), key=sort_key)
        self.assertEqual(result_data, expected_data)

    def test_dropduplicates_default(self):
        """
        Test case to validate the default behavior of 'drop_duplicates' method.
        """
        result_vd = self.validator_test.drop_duplicates(
            self.input_columns[0], self.input_columns[1])
        expected_output = [('Paula', '30'),
                           ('Marlene', None),
                           ('Paula', '27'),
                           ('Juan', '30'),
                           (None, '67')]
        expected_columns = ['nombre', 'edad']
        self.validate_result(result_vd=result_vd,
                             expected_output=expected_output,
                             expected_columns=expected_columns)


if __name__ == '__main__':
    unittest.main()
