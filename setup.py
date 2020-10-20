#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-searchads360',
    version='0.0.4',
    description='Singer.io tap for extracting data from google search ads API',
    author='Reeport',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_searchads360'],
    install_requires=[
        'singer-python>=5.9.0',
        'requests>=2.20.0',
        'pandas>=0.23.4'
    ],
    entry_points='''
        [console_scripts]
        tap-searchads360=tap_searchads360:main
    ''',
    packages=['tap_searchads360'],
    package_data={
        'tap_searchads360': ['schemas/*.json']
    },
    include_package_data=True
)
