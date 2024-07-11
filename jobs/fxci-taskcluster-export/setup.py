#!/usr/bin/env python

from setuptools import setup, find_packages

README = open("README.md").read()

setup(
    name="fxci-etl",
    version="0.1.0",
    author="ahalberstadt@mozilla.com",
    packages=find_packages(),
    entry_points={"console_scripts": ["fxci-etl = fxci_etl.console:run"]},
    long_description=README,
    include_package_data=True,
    license="MPL 2.0",
)
