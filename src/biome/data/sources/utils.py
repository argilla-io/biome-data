import os.path
from typing import Tuple, List, Optional, Dict, Union

import dask.dataframe as dd
import pandas as pd

import yaml


def extension_from_path(path: Union[str, List[str]]) -> str:
    """Helper method to get file extension

    Parameters
    ----------
    path
        A string or a list of strings.
        If it is a list, the first entry is taken.

    Returns
    -------
    extension
        File extension
    """
    if isinstance(path, str):
        path = [path]

    _, extension = os.path.splitext(path[0])

    return extension.lower()[1:]  # skip first char, which is a dot


def make_paths_relative(yaml_dirname: str, cfg_dict: Dict, path_keys: List[str] = None):
    """Helper method to convert file system paths relative to the yaml config file,
    to paths relative to the current path.

    It will recursively cycle through `cfg_dict` if it is nested.

    Parameters
    ----------
    yaml_dirname
        Dirname to the yaml config file (as obtained by `os.path.dirname`.
    cfg_dict
        The config dictionary extracted from the yaml file.
    path_keys
        If not None, it will only try to modify the `cfg_dict` values corresponding to the `path_keys`.
    """
    for key, value in cfg_dict.items():
        if isinstance(value, dict):
            make_paths_relative(yaml_dirname, value, path_keys)

        if path_keys and key not in path_keys:
            continue

        if is_relative_file_system_path(value):  # returns False if value is not a str
            cfg_dict[key] = os.path.join(yaml_dirname, value)

        # cover lists as well
        if isinstance(value, list):
            cfg_dict[key] = [
                os.path.join(yaml_dirname, path)
                if is_relative_file_system_path(path)
                else path
                for path in value
            ]


def is_relative_file_system_path(string: str) -> bool:
    """Helper method to check if a string is a relative file system path.

    Parameters
    ----------
    string
        The string to be checked.

    Returns
    -------
    bool
        Whether the string is a relative file system path or not.
        If string is not type(str), return False.
    """
    if not isinstance(string, str):
        return False
    # we require the files to have a file name extension ... ¯\_(ツ)_/¯
    if not extension_from_path(string):
        return False
    # check if a domain name
    if string.lower().startswith(
        ("http://", "https://", "ftp://", "sftp://", "s3://", "hdfs://", "gs://")
    ):
        return False
    # check if an absolute path
    if os.path.isabs(string):
        return False
    return True


def _dict_to_list(row: List[Dict]) -> Optional[dict]:
    """ Converts a list of structured data into a dict of list, where every dict key
        is the list aggregation for every key in original dict

        For example:

        l = [{"name": "Frank", "lastName":"Ocean"},{"name":"Oliver","lastName":"Sacks"]
        _dict_to_list(l)
        {"name":["Frank","Oliver"], "lastName":["Ocean", "Sacks"]}
    """
    try:
        for row_i in row:
            if isinstance(row_i, list):
                row = row_i
        return pd.DataFrame(row).to_dict(orient="list")
    except (ValueError, TypeError):
        return None


def _columns_analysis(
    data_frame: pd.DataFrame
) -> Tuple[List[str], List[str], List[str]]:
    dicts = []
    lists = []
    unmodified = []

    def is_list_of_structured_data(elem) -> bool:
        if isinstance(elem, list):
            for elem_i in elem:
                if isinstance(elem_i, (dict, list)):
                    return True
        return False

    for column in data_frame.columns:
        column_data = data_frame[column].dropna()
        element = column_data.iloc[0] if not column_data.empty else None

        current_list = unmodified
        if isinstance(element, dict):
            current_list = dicts
        elif is_list_of_structured_data(element):
            current_list = lists
        current_list.append(column)

    return dicts, lists, unmodified


def flatten_dask_dataframe(data_frame: dd.DataFrame) -> dd.DataFrame:
    """
    Flatten an dataframe adding nested values as new columns
    and dropping the old ones
    Parameters
    ----------
    data_frame
        The original dask DataFrame

    Returns
    -------

    A new Dataframe with flatten content

    """
    # We must materialize some data for compound the new flatten DataFrame
    meta_flatten = flatten_dataframe(data_frame.head(1))

    def _flatten_stage(data_frame_i: pd.DataFrame) -> pd.DataFrame:
        new_df = flatten_dataframe(data_frame_i)
        for column in new_df.columns:
            # we append the new columns to the original dataframe
            data_frame_i[column] = new_df[column]

        return data_frame_i

    data_frame = data_frame.map_partitions(
        _flatten_stage,
        meta={**data_frame.dtypes.to_dict(), **meta_flatten.dtypes.to_dict()},
    )
    return data_frame[meta_flatten.columns]


def flatten_dataframe(data_frame: pd.DataFrame) -> pd.DataFrame:
    dict_columns, list_columns, unmodified_columns = _columns_analysis(data_frame)

    if len(data_frame.columns) == len(unmodified_columns):
        return data_frame

    dfs = []
    for column in list_columns:
        column_df = pd.DataFrame(
            data=[data for data in data_frame[column].apply(_dict_to_list) if data],
            index=data_frame.index,
        )
        column_df.columns = [
            f"{column}.*.{column_df_column}" for column_df_column in column_df.columns
        ]
        dfs.append(column_df)

    for column in dict_columns:
        column_df = pd.DataFrame(
            data=[data if data else {} for data in data_frame[column]],
            index=data_frame.index,
        )
        column_df.columns = [
            f"{column}.{column_df_column}" for column_df_column in column_df.columns
        ]
        dfs.append(column_df)

    flatten = flatten_dataframe(pd.concat(dfs, axis=1))
    return pd.concat([data_frame[unmodified_columns], flatten], axis=1)


def save_dict_as_yaml(dictionary: dict, path: str, create_dirs: bool = True) -> str:
    """Save a cfg dict to path as yaml

    Parameters
    ----------
    dictionary
        Dictionary to be saved
    path
        Filesystem location where the yaml file will be saved
    create_dirs
        If true, create directories in path.
        If false, throw exception if directories in path do not exist.

    Returns
    -------
    path
        Location of the yaml file
    """
    dir_name = os.path.dirname(path)
    if not os.path.isdir(dir_name):
        if not create_dirs:
            raise NotADirectoryError(f"Path '{dir_name}' does not exist.")
        os.makedirs(dir_name)

    with open(path, "w") as yml_file:
        yaml.dump(dictionary, yml_file, default_flow_style=False)

    return path
