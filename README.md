# DQ Launcher - Version 1.0
DQ Launcher is a Python library that provides functions for performing data quality validations on Spark DataFrames. This library can help you ensure the integrity, consistency, and accuracy of your data before conducting analysis or training models with it.

## Installation
You can install DQ Launcher using 'pip':
pip install dqlauncher

## Usage
First, import the DQ Launcher Session class and the Validator classes:
from dqlauncher.session import SparkDQLauncher

You need to initialize a SparkDQLauncher object. It is a heritage class from SparkSession that allows you to create Validator objects in Spark context.

## Initialization
You can initialize a Validator object as follows:
dq_launcher = SparkDQLauncher(name_of your_app)

validator = dq_launcher.CreateValidator(data, columns)

Now you have a Validator Object. It is a full functional PysPark.DataFrame with added functionalities. You can use all methods and features a DataFrame can handle, and perform Data Quality Validations over this object.

## Basic Validations
- Check for null values
- Check for unique values
- Validate data types

## Advanced Validations
- Validate fields against a reference table
- Validate data length
- Standardize names

## Statistics
- Count and get the percentage of null values
- Count and get the percentage of unique values

## Contribution
If you want to contribute to this project, please refer to: CONTRIBUTING.md

## License
This project is licensed under the MIT License. See the LICENSE file for more details.

### Author
Pablo Sierra Lorente

### Year
2023
