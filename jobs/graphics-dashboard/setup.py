from setuptools import setup, find_packages

setup(
    name="graphics-dashboard",
    version="0.1.0",
    packages=find_packages(include=["graphics_dashboard", "graphics_dashboard.*"]),
    include_package_data=True,
    package_data={"graphics_dashboard": ["sql/*.sql"]},
    license="MPL 2.0",
)
