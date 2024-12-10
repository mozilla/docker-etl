#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="kevel-metadata-job",
    version="0.1.0",
    author="cmorales@mozilla.com",
    packages=find_packages(include=["kevel_metadata"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
