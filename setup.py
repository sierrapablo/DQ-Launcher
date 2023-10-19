from setuptools import setup, find_packages

setup(
    name='pyquality',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'numpy==1.26.1',
        'pandas==2.1.1',
        'pyspark==3.5.0',
        'py4j==0.10.9.7',
        'python-dateutil>=2.8.2',
        'pytz>=2023.3.post1',
        'six>=1.16.0',
        'tzdata>=2023.3'
    ],
    author='Pablo Sierra Lorente',
    author_email='pablosierralorente@outlook.es',
    description='Qualiti functions over PySpark DataFrames',
    url='https://github.com/sierrapablo/PyQuality',
    license='MIT', # MIT license since 2023/10/10
    keywords='dataquality, bigdata, validations',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.11.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    include_package_data=True,
    zip_safe=False
)
