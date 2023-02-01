#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = []

test_requirements = [
    "pytest>=3",
]

setup(
    author="cls-python",
    author_email="maintainer@cls-python.org ",
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    description="Python implementation of the cls framework. ",
    install_requires=requirements,
    license="TODO",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="cls_python",
    name="cls_python",
    packages=find_packages(include=["cls_python", "cls_python.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/Jekannadar/cls_python",
    version="0.1.0",
    zip_safe=False,
)