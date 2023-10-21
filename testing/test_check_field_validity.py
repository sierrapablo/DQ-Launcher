import unittest
from pyspark.sql import SparkSession
from dqlauncher.validator import Validator
from dqlauncher.utilities.errors import ValidationError


class CheckFieldValidityTest(unittest.TestCase):
    """
    A test case class for testing the check_field_validity method in the Validator class.
    """
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test check_field_validity") \
            .getOrCreate()

    def test_checkvalidity_valid_true_df(self):
        """
        Test case for check_field_validity method with valid=True and DataFrame reference.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)

        result_vd = test_vd.check_field_validity(column='column1',
                                                 ref_df=ref_df,
                                                 field_ref='column_ref',
                                                 valid=True)
        expected_output = [('abc', 1),
                           ('def', 0),
                           ('ghi', 1),
                           ('jkl', 0)]
        expected_columns = ['column1', 'column1_VALID']

        result_data = result_vd.df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_valid_true_vd(self):
        """
        Test case for check_field_validity method with valid=True and Validator reference.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_vd = Validator(ref_df)

        result_vd = test_vd.check_field_validity(column='column1',
                                                 ref_df=ref_vd,
                                                 field_ref='column_ref',
                                                 valid=True)
        expected_output = [('abc', 1),
                           ('def', 0),
                           ('ghi', 1),
                           ('jkl', 0)]
        expected_columns = ['column1', 'column1_VALID']

        result_data = result_vd.df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_valid_false(self):
        """
        Test case for check_field_validity method with valid=False.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_vd = Validator(ref_df)

        result_vd = test_vd.check_field_validity(column='column1',
                                                 ref_df=ref_vd,
                                                 field_ref='column_ref',
                                                 valid=False)
        expected_output = [('NOT VALID', ),
                           ('def', ),
                           ('NOT VALID', ),
                           ('jkl', )]
        expected_columns = ['column1']

        result_data = result_vd.df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_valid_true_defcol(self):
        """
        Test case for check_field_validity method with valid=True and custom result column name.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_vd = Validator(ref_df)

        result_vd = test_vd.check_field_validity(column='column1',
                                                 ref_df=ref_vd,
                                                 field_ref='column_ref',
                                                 valid=True,
                                                 result_column='RESULT')
        expected_output = [('abc', 1),
                           ('def', 0),
                           ('ghi', 1),
                           ('jkl', 0)]
        expected_columns = ['column1', 'RESULT']

        result_data = result_vd.df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_valid_false_defcol(self):
        """
        Test case for check_field_validity method with valid=False and custom result column name.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_vd = Validator(ref_df)

        result_vd = test_vd.check_field_validity(column='column1',
                                                 ref_df=ref_vd,
                                                 field_ref='column_ref',
                                                 valid=False,
                                                 result_column='RESULT')
        expected_output = [('NOT VALID', ),
                           ('def', ),
                           ('NOT VALID', ),
                           ('jkl', )]
        expected_columns = ['column1']

        result_data = result_vd.df.collect()
        expected_data = self.spark.createDataFrame(
            expected_output,
            schema=expected_columns
        ).collect()

        self.assertEqual(result_data, expected_data)

    def test_checkvalidity_columnerror(self):
        """
        Test case for check_field_validity method when an invalid reference column is provided.
        This test fails because it reports correctly the raised Exception.
        """
        input_data = [('abc',),
                      ('def',),
                      ('ghi',),
                      ('jkl',)]
        columns = ['column1']
        test_df = self.spark.createDataFrame(input_data, schema=columns)
        test_vd = Validator(test_df)

        ref_data = [('xyz',),
                    ('ghi',),
                    ('abc',)]
        ref_columns = ['column_ref']

        ref_df = self.spark.createDataFrame(ref_data, schema=ref_columns)
        ref_vd = Validator(ref_df)

        with self.assertRaises(ValidationError) as context:
            test_vd.check_field_validity(column='column1',
                                         ref_df=ref_vd,
                                         field_ref='reference',
                                         valid=True)


if __name__ == "__main__":
    unittest.main()
