import unittest
from pyspark.sql import SparkSession
from dqlauncher.validator import Validator
from dqlauncher.utilities.errors import ValidationError


class CheckFieldValidityTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test check_field_validity") \
            .getOrCreate()

    def test_checkvalidity_valid_true_df(self):

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

        expected_error_msg = "Column 'reference' not found. Columns present: ['column_ref']"

        self.assertEqual(str(context.exception), expected_error_msg)


if __name__ == "__main__":
    unittest.main()
