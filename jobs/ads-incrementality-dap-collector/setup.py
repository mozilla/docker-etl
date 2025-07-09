#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="ads-incrementality-dap-collector",
    version="0.1.0",
    author="gleonard@mozilla.com",
    packages=find_packages(include=["ads-incrementality-dap-collector"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
