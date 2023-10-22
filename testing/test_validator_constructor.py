import unittest
from dqlauncher.validator import Validator
from dqlauncher.session import DQLauncherSession

class TestValidatorConstructor(unittest.TestCase):

    def setUp(self):
        self.input_data = [('Paula', '30'),
                           ('Marlene', None),
                           (None, '67'),
                           ('Juan', '30')]
        self.columns = ['nombre', 'edad']

    def test_validator_via_dataframe(self):
        launcher = DQLauncherSession()
        launcher_df = launcher.spark.createDataFrame(self.input_data, schema=self.columns)
        validator_test = Validator(launcher_df)
        self.assertIsInstance(validator_test, Validator)

    def test_validator_via_createValidator(self):
        launcher = DQLauncherSession()
        validator_test = launcher.createValidator(self.input_data, schema=self.columns)
        self.assertIsInstance(validator_test, Validator)

if __name__ == '__main__':
    unittest.main()