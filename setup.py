# -*- coding: utf-8 -*-
# @Time    : 29/4/2020 11:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: setup.py
# @Software: PyCharm

"""
Either specify package data in setup.py or MANIFEST.in:
https://www.codenong.com/cs106808509/
"""

from setuptools import setup, find_packages

setup(
    name='qtrader',
    version='0.0.2',
    keywords=('setup', 'qtrader'),
    description='setup qtrader',
    long_description='',
    license='MIT',
    install_requires=['sqlalchemy',
                      'pandas',
                      'numpy',
                      'pytz',
                      'clickhouse-driver',
                      'matplotlib',
                      'plotly'],
    author='josephchen',
    author_email='josephchenhk@gmail.com',
    include_package_data=True,
    packages=find_packages(),
    # package_data={"": [
    # "*.ico",
    # "*.ini",
    # "*.dll",
    # "*.so",
    # "*.pyd",
    # ]},
    platforms='any',
    url='',
    entry_points={
        'console_scripts': [
            'example=examples.demo_strategy:run'
        ]
    },
)
