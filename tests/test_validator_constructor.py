import unittest
from unittest.mock import MagicMock
from pyquality.pyquality import Validator
from pyquality.utilities.errors import SparkSessionError


class TestConstructorPyQuality(unittest.TestCase):

    def test_constructor_with_spark(self):
        spark_mock = MagicMock()
        validator_test = Validator(spark=spark_mock)
        self.assertEqual(validator_test.spark, spark_mock)

    def test_constructor_without_spark(self):
        validator_test = Validator()
        self.assertIsNotNone(validator_test.spark)

    def test_close_with_spark(self):
        spark_mock = MagicMock()
        validator_test = Validator(spark=spark_mock)
        validator_test.close()
        spark_mock.stop.assert_called_once()

    def test_close_without_spark(self):
        validator_test = Validator()
        validator_test.spark = None
        with self.assertRaises(SparkSessionError):
            validator_test.close()


if __name__ == '__main__':
    unittest.main()
