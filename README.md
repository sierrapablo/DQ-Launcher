# PyQuality! - Version 0.1
PyQuality is a Python library that provides functions for performing data quality validations on Spark DataFrames. This library can help you ensure the integrity, consistency, and accuracy of your data before conducting analysis or training models with it.

## Installation
You can install PyQuality using 'pip':
pip install pyquality

## Usage
First, import the PyQuality class:
from pyquality import PyQuality

## Initialization
You can initialize a PyQuality object as follows:
validator = PyQuality(spark_session)

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
