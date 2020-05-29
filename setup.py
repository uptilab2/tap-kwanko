#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-kwanko",
    version="0.1.0",
    description="Singer.io tap for extracting data from kwanko",
    author="Mounir Yahyaoui",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_kwanko"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-kwanko=tap_kwanko:main
    """,
    packages=["tap_kwanko"],
    package_data={
        "schemas": ["tap_kwanko/schemas/*.json"]
    },
    include_package_data=True,
)
