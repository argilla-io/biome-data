import glob
import logging
from glob import glob
from typing import Dict, Optional, Union, List

import dask.dataframe as dd
import flatdict
import pandas as pd
from dask import delayed
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from biome.data.sources.utils import flatten_dataframe

_logger = logging.getLogger(__name__)
# TODO: The idea is to make the readers a class and define a metaclass that they have to follow.
#       For now, all reader methods have to return a dask.DataFrame


def from_csv(path: Union[str, List[str]], **params) -> dd.DataFrame:
    """Creates a `dask.DataFrame` from one or several csv files.
    Includes a "path column".

    Parameters
    ----------
    path
        Path to files
    params
        Extra arguments passed on to `dask.dataframe.read_csv`

    Returns
    -------
    df
        A `dask.DataFrame`

    """
    return dd.read_csv(path, include_path_column=True, **params)


def from_json(
    path: Union[str, List[str]], flatten: bool = True, **params
) -> dd.DataFrame:
    """Creates a `dask.DataFrame` from one or several json files.
    Includes a "path column".

    Parameters
    ----------
    path
        Path to files
    flatten
        If true (default false), flatten json nested data
    params
        Extra arguments passed on to `pandas.read_json`

    Returns
    -------
    df
        A `dask.DataFrame`
    """

    def json_engine(*args, flatten: bool = False, **kwargs) -> pd.DataFrame:
        df = pd.read_json(*args, **kwargs)
        return flatten_dataframe(df) if flatten else df

    path_list = _get_file_paths(path)

    dds = []
    for path_name in path_list:
        ddf = dd.read_json(path_name, flatten=flatten, engine=json_engine, **params)
        ddf["path"] = path_name
        dds.append(ddf)

    return dd.concat(dds)


def from_parquet(path: Union[str, List[str]], **params) -> dd.DataFrame:
    """Creates a `dask.DataFrame` from one or several parquet files.
    Includes a "path column".

    Parameters
    ----------
    path
        Path to files
    params
        Extra arguments passed on to `pandas.read_parquet`

    Returns
    -------
    df
        A `dask.DataFrame`
    """
    path_list = _get_file_paths(path)

    dds = []
    for path_name in path_list:
        ddf = dd.read_parquet(path_name, **params, engine="pyarrow")
        ddf["path"] = path_name
        dds.append(ddf)

    return dd.concat(dds)


def from_excel(path: Union[str, List[str]], **params) -> dd.DataFrame:
    """Creates a `dask.DataFrame` from one or several excel files.
    Includes a "path column".

    Parameters
    ----------
    path
        Path to files
    params
        Extra arguments passed on to `pandas.read_excel`

    Returns
    -------
    df
        A `dask.DataFrame`
    """
    path_list = _get_file_paths(path)

    dds = []
    for path_name in path_list:
        parts = delayed(pd.read_excel)(path_name, **params)
        df = dd.from_delayed(parts).fillna("")
        df["path"] = path_name
        dds.append(df)

    return dd.concat(dds)


def _get_file_paths(paths: Union[str, List[str]]) -> List[str]:
    """Return a list of path names that match the path names in paths.
    The path names can contain shell-style wildcards.

    Parameters
    ----------
    paths
        A path name or a list of path names. These path names can contain wildcards.

    Returns
    -------
    list_of_paths
        A list of path names.
    """
    if isinstance(paths, str):
        return glob(paths)
    path_lists = [glob(path) for path in paths]

    # flatten the list of lists
    return [path for sublist in path_lists for path in sublist]


def from_elasticsearch(
    query: Optional[Dict] = None,
    npartitions: int = 2,
    client_cls: Optional = None,
    client_kwargs=None,
    **kwargs,
) -> dd.DataFrame:
    """Reads documents from Elasticsearch.

    By default, documents are sorted by ``_doc``. For more information see the
    scrolling section in Elasticsearch documentation.

    Parameters
    ----------
    query : dict, optional
        Search query.
    npartitions : int, optional
        Number of partitions, default is 2.
    client_cls : elasticsearch.Elasticsearch, optional
        Elasticsearch client class.
    client_kwargs : dict, optional
        Elasticsearch client parameters.
    **params
        Additional keyword arguments are passed to the the
        ``elasticsearch.helpers.scan`` function.

    Returns
    -------
    out : List[Delayed]
        A list of ``dask.Delayed`` objects.

    Examples
    --------

    Get all documents in elasticsearch.
    >>> docs = from_elasticsearch()

    Get documents matching a given query.
    >>> query = {"query": {"match_all": {}}}
    >>> docs = from_elasticsearch(query, index="myindex", doc_type="stuff")

    """

    if npartitions < 2:
        _logger.warning(
            "A minium of 2 partitions is needed for elasticsearch scan slices....Setting partitions number to 2"
        )
        npartitions = 2

    query = query or {}
    # Sorting by _doc is preferred for scrolling.
    query.setdefault("sort", ["_doc"])
    if client_cls is None:
        client_cls = Elasticsearch
    # We load documents in parallel using the scrolling + slicing feature.
    index_scan = [
        delayed(_elasticsearch_scan)(client_cls, client_kwargs, **scan_kwargs)
        for scan_kwargs in [
            dict(kwargs, query=dict(query, slice=slice))
            for slice in [{"id": idx, "max": npartitions} for idx in range(npartitions)]
        ]
    ]

    return dd.from_delayed(index_scan)


def _elasticsearch_scan(client_cls, client_kwargs, **params) -> pd.DataFrame:
    def map_to_source(x: Dict) -> Dict:
        flat = flatdict.FlatDict(
            {**x["_source"], **dict(id=x["_id"], resource=x["_index"])}, delimiter="."
        )
        return dict(flat)

    # This method is executed in the worker's process and here we instantiate
    # the ES client as it cannot be serialized.
    # TODO check empty DataFrame
    client = client_cls(**(client_kwargs or {}))
    df = pd.DataFrame((map_to_source(document) for document in scan(client, **params)))

    return df.set_index("id") if not df.empty else df
