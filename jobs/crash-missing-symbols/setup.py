from setuptools import setup, find_packages

setup(
    name="crash-missing-symbols",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(
        include=["crash_missing_symbols", "crash_missing_symbols.*"]
    ),
    include_package_data=True,
    package_data={"crash_missing_symbols": ["sql/*.sql"]},
    license="MPL 2.0",
)
