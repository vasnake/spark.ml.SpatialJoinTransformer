# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from codecs import open
from os import path
import re

here = path.abspath(path.dirname(__file__))

# val Version = "0.0.2-SNAPSHOT"
with open(path.join(here, '..', '..', '..', 'build.sbt')) as f:
    VERSION = re.search('val Version = \"(.+)\"', f.read(), re.IGNORECASE).group(1)
    print("VERSION: {}".format(VERSION))

with open('dependencies.txt') as f:
    required_packages = f.read().splitlines()

setup(
    name='spark-transformer-spatialjoin',
    version=VERSION,
    description='Spark ml transformer',
    author='Valentin Fedulov',
    author_email='vasnake@gmail.com',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Data Processing :: ML :: Apache Spark',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5'
    ],
    keywords='spark pyspark sparkml ml transformer pipeline spatial join geo',
    packages=find_packages(exclude=['contrib', 'docs', 'test*']),
    install_requires=required_packages
)
