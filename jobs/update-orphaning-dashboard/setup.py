#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="update-orphaning-dashboard",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(
        include=["update_orphaning_dashboard", "update_orphaning_dashboard.*"]
    ),
    long_description=readme,
    include_package_data=True,
    package_data={"update_orphaning_dashboard": ["sql/*.sql"]},
    license="MPL 2.0",
)
