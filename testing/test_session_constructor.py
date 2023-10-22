import unittest
from pyspark.sql import SparkSession
from dqlauncher.session import DQLauncherSession


class TestDQLauncherSessionConstructor(unittest.TestCase):

    def test_default_constructor(self):
        launcher = DQLauncherSession(
            appName='TestDQLauncher', master='local[1]', spark_config=None)
        self.assertIsInstance(launcher, DQLauncherSession)
        self.assertIsInstance(launcher.spark, SparkSession)

    def test_custom_config_constructor(self):
        custom_config = {"spark.executor.memory": "2g",
                         "spark.executor.cores": "4"}
        launcher = DQLauncherSession(
            appName='TestDQLauncher', spark_config=custom_config)
        self.assertIsInstance(launcher, DQLauncherSession)
        self.assertIsInstance(launcher.spark, SparkSession)
        self.assertEqual(launcher.spark.conf.get("spark.executor.memory"), "2g")
        self.assertEqual(launcher.spark.conf.get("spark.executor.cores"), "4")

    def test_local_master_constructor(self):
        launcher = DQLauncherSession(
            appName='TestDQLauncher', master='local[2]')
        self.assertIsInstance(launcher, DQLauncherSession)
        self.assertIsInstance(launcher.spark, SparkSession)
        self.assertEqual(launcher.spark.conf.get("spark.master"), 'local[2]')

if __name__ == '__main__':
    unittest.main()
