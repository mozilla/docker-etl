from setuptools import setup, find_packages

setup(
    name="highwind",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["highwind", "highwind.*"]),
    include_package_data=True,
    license="MPL 2.0",
)
