# -*- coding: utf-8 -*-
# @Time    : 29/4/2020 11:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: setup.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

# Either specify package data in setup.py or MANIFEST.in:
# https://www.codenong.com/cs106808509/

from setuptools import setup, find_packages
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name='qtrader',
    version='0.0.4',
    keywords=('Quantitative Trading', 'Qtrader', 'Backtest'),
    description='Qtrader: Event-Driven Algorithmic Trading Engine',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='JXW',
    install_requires=['sqlalchemy',
                      'pandas',
                      'numpy',
                      'pytz',
                      'clickhouse-driver',
                      'matplotlib',
                      'plotly',
                      'python-telegram-bot',
                      'dash'],
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
