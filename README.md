# Biome-data
> Biome-data is the core data source managemente for [biome-text](https://github.com/recognai/biome-text)


## Features
* Read data source in a homogeneous way
* Extensible
* Scalable
...

## Install
Biome-data supports Python 3.6 and can be installed using pip and conda.

## pip
For installing biome-data with pip, it is highly recommended to install packages in a virtual environment to avoid conflicts with other installations and system packages.

```bash
python -m venv .env
source .env/bin/activate
pip install --upgrade pip
pip install https://github.com/recognai/biome-data.git
```

## conda
We provide a conda environment to install most of the package dependencies. We require you to clone this repository and run: 

```bash
conda env create -f environment.yml
conda activate biome-data
make dev
```

## Working with Biome data: DataSource class

TBD

## Licensing

The code in this project is licensed under Apache 2 license.


