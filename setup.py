#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup, find_packages


def about_info(package: str):
    """Fetch about info """
    root = os.path.abspath(os.path.dirname(__file__))
    with open(
        os.path.join(root, "src", package.replace("-", "/"), "about.py"),
        encoding="utf8",
    ) as f:
        about = {}
        exec(f.read(), about)
        return about


if __name__ == "__main__":
    package_name = "biome-data"
    about = about_info(package_name)
    setup(
        version=about["__version__"],
        name=package_name,
        description="Biome-data is a common module for data source manipulation",
        author="Recognai",
        author_email="francisco@recogn.ai",
        url="https://www.recogn.ai/",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        packages=find_packages("src"),
        package_dir={"": "src"},
        install_requires=[
            "dask[complete]~=2.0",
            "cachey~=0.1.0",  # required by dask.cache
            "pyarrow~=0.14",
            "ujson~=1.35",
            "pandas~=0.25.0",
            "elasticsearch<7.0",  # latest version doesn't work with dask-elk module
            "dask-elk~=0.2.0",
            "bokeh~=1.3",
            "xlrd~=1.2",
            "flatdict~=3.4",
        ],
        extras_require={"testing": ["pytest", "pytest-cov", "pytest-pylint", "black"]},
        python_requires=">=3.6.1",
        zip_safe=False,
    )
