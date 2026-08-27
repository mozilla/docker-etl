from setuptools import setup, find_packages

setup(
    name="crash-correlations",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["crash_correlations", "crash_correlations.*"]),
    include_package_data=True,
    package_data={"crash_correlations": ["sql/*.sql"]},
    license="MPL 2.0",
)
