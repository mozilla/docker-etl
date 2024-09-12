#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="ad_campaign_metrics_importer",
    version="0.1.0",
    author="ksantos@mozilla.com",
    packages=find_packages(include=["ad_campaign_metrics_importer"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
