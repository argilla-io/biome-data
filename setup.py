#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

if __name__ == "__main__":

    setup(
        version="0.1.dev",
        name="biome-data",
        description="Biome-data is a commmon module for data source manipulation",
        author="Recognai",
        author_email="francisco@recogn.ai",
        url="https://www.recogn.ai/",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        packages=find_packages("src"),
        package_dir={"": "src"},
        install_requires=[
            "dask[complete]~=2.0",
            "cachey~=0.1",  # required by dask.cache
            "pyarrow~=0.14",
            "ujson~=1.35",
            "pandas~=0.25.0",
            "elasticsearch~=6.0",
            "bokeh~=1.3",
            "xlrd~=1.2",
            "flatdict~=3.4",
        ],
        extras_require={"testing": ["pytest", "pytest-cov", "pytest-pylint"]},
        python_requires=">=3.6.1",
        zip_safe=False,
    )
