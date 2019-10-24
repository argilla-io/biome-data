import logging
import os.path
import warnings
from typing import Dict, Callable, Any, Union, List, Optional, Tuple

import yaml
from dask.bag import Bag
import dask.dataframe as dd

from .readers import (
    ID,
    RESOURCE,
    PATH_COLUMN_NAME,
    from_csv,
    from_json,
    from_excel,
    from_parquet,
    ElasticsearchDataFrameReader,
)
from .utils import make_paths_relative


class DataSource:
    """This class takes care of reading the data source, usually specified in a yaml file.

    It uses the *source readers* to extract a dask DataFrame.

    Parameters
    ----------
    source
        The data source. Could be a list of filesystem path, or a key name indicating the source backend (elasticsearch)
    attributes
        Attributes needed for extract data from source
    format
        The data format. Optional. If found, overwrite the format extracted from source.
        Supported formats are listed as keys in the `SUPPORTED_FORMATS` dict of this class.
    mapping
        Used to map the features (columns) of the data source
        to the parameters of the DatasetReader's `text_to_instance` method.
    kwargs
        Additional kwargs are passed on to the *source readers* that depend on the format.
        @Deprecated. Use `attributes` instead
    """

    _logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

    SUPPORTED_FORMATS = {
        "xls": (from_excel, dict(na_filter=False, keep_default_na=False, dtype=str)),
        "xlsx": (from_excel, dict(na_filter=False, keep_default_na=False, dtype=str)),
        "csv": (from_csv, dict(assume_missing=False, na_filter=False, dtype=str)),
        "json": (from_json, dict()),
        "jsonl": (from_json, dict()),
        "json-l": (from_json, dict()),
        "parquet": (from_parquet, dict()),
        # No file system based readers
        ElasticsearchDataFrameReader.SOURCE_TYPE: (
            ElasticsearchDataFrameReader.read,
            dict(),
        ),
    }
    # maps the supported formats to the corresponding "source readers"

    def __init__(
        self,
        source: Optional[Union[str, List[str]]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        mapping: Optional[Dict[str, Union[List[str], str]]] = None,
        format: Optional[str] = None,
        **kwargs,
    ):

        if kwargs:
            warnings.warn(
                "Passing keyword arguments is deprecated and will be disabled."
                " Please, use attributes argument instead",
                DeprecationWarning,
            )

        attributes = attributes or {}
        kwargs = kwargs or {}

        if not format and source:
            format = self.__format_from_source(source)

        source_reader, defaults = self._find_reader(format)
        reader_arguments = {**defaults, **kwargs, **attributes}

        df = (
            source_reader(source, **reader_arguments)
            if source
            else source_reader(**reader_arguments)
        )
        df = df.dropna(how="all").rename(
            columns={column: column.strip() for column in df.columns.astype(str).values}
        )
        # TODO allow disable index reindex
        if "id" in df.columns:
            df = df.set_index("id")

        self._df = df
        self.mapping = mapping or {}

    @classmethod
    def add_supported_format(
        cls, format_key: str, parser: Callable, default_params: Dict[str, Any] = None
    ) -> None:
        """Add a new format and reader to the data source readers.

        Parameters
        ----------
        format_key
            The new format key
        parser
            The parser function
        default_params
            Default parameters for the parser function
        """
        if format_key in cls.SUPPORTED_FORMATS.keys():
            cls._logger.warning("Already defined format {}".format(format_key))
            pass

        cls.SUPPORTED_FORMATS[format_key] = (parser, default_params or {})

    def to_bag(self) -> Bag:
        """Turns the DataFrame of the data source into a `dask.Bag` of dictionaries, one dict for each row.
        Each dictionary has the column names as keys.

        Returns
        -------
        bag
            A `dask.Bag` of dicts.
        """
        dict_keys = [str(column).strip() for column in self._df.columns]

        return self._df.to_bag(index=True).map(self._row2dict, columns=dict_keys)

    def to_mapped_bag(self) -> Bag:
        """Turns the mapped DataFrame of the data source into a `dask.Bag` of dictionaries, one dict for each row.
        Each dictionary has the column names as keys.

        Returns
        -------
        bag
            A `dask.Bag` of dicts.
        """
        mapped_df = self.to_mapped_dataframe()
        dict_keys = [str(column).strip() for column in mapped_df.columns]
        return mapped_df.to_bag(index=True).map(self._row2dict, columns=dict_keys)

    def to_dataframe(self) -> dd.DataFrame:
        """Returns the underlying DataFrame of the data source"""
        return self._df

    def to_mapped_dataframe(self) -> dd.DataFrame:
        """The columns of this DataFrame are named after the mapping keys, which in turn should match
        the parameter names in the DatasetReader's `text_to_instance` method.
        The content of these columns is specified in the mapping dictionary.

        Returns
        -------
        mapped_dataframe
            Contains columns corresponding to the parameter names of the DatasetReader's `text_to_instance` method.
        """
        if not self.mapping:
            raise ValueError("For a mapped DataFrame you need to specify a mapping!")

        # This is strictly a shallow copy of the underlying computational graph
        mapped_dataframe = self._df.copy()

        for parameter_name, data_features in self.mapping.items():
            # convert to list, otherwise the axis=1 raises an error with the returned pd.Series in the try statement
            # if no header is present in the source data, the column names are ints
            if isinstance(data_features, (str, int)):
                data_features = [data_features]

            try:
                mapped_dataframe[parameter_name] = self._df.loc[:, data_features].apply(
                    self._to_dict_or_any, axis=1, meta=(None, "object")
                )
            except KeyError as e:
                raise KeyError(e, f"Did not find {data_features} in the data source!")

        return mapped_dataframe.loc[:, list(self.mapping.keys())]

    @staticmethod
    def _to_dict_or_any(value: dd.Series) -> Union[Dict, Any]:
        """Transform a `dask.dataframe.Series` of strings to a dict or a str, depending on its length."""
        if len(value) > 1:
            return value.to_dict()
        else:
            return value.iloc[0]

    @staticmethod
    def _row2dict(
        row: Tuple, columns: List[str], default_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """ Convert a pandas row into a dict object """
        id = row[0]
        data = row[1:]

        # For duplicated column names, pandas append a index prefix with dots '.' We prevent
        # index failures by replacing for '_'
        sanitized_columns = [column.replace(".", "_") for column in columns]
        data = dict([(ID, id)] + list(zip(sanitized_columns, data)))

        # DataFrame.read_csv allows include path column called `path`
        data[RESOURCE] = data.get(
            RESOURCE, data.get(PATH_COLUMN_NAME, str(default_path))
        )

        return data

    @classmethod
    def from_yaml(cls: "DataSource", file_path: str) -> "DataSource":
        """Create a data source from a yaml file.

        Parameters
        ----------
        file_path
            The path to the yaml file.

        Returns
        -------
        cls
        """
        with open(file_path) as yaml_file:
            cfg_dict = yaml.safe_load(yaml_file)

        # File system paths are usually specified relative to the yaml config file -> they have to be modified
        # path_keys is not necessary, but specifying the dict keys
        # (for which we check for relative paths) is a safer choice
        path_keys = ["path", "metadata_file"]
        make_paths_relative(os.path.dirname(file_path), cfg_dict, path_keys=path_keys)

        mapping = cfg_dict.pop("mapping", None)
        # backward compatibility
        if not mapping:
            try:
                mapping = cfg_dict.pop("forward")
                warnings.warn(
                    "The key 'forward' is deprecated! Please use the 'mapping' key in the future.",
                    DeprecationWarning,
                )
            except KeyError:
                pass

        mapping = cls._make_backward_compatible(mapping) if mapping else None

        return cls(**cfg_dict, mapping=mapping)

    @staticmethod
    def _make_backward_compatible(mapping: Dict) -> Dict:
        """Makes the mapping section of a data source yml file backward compatible.
        For a 1.0 version, this method can be removed.

        Parameters
        ----------
        mapping
            The mapping dict of the data source yml
        """
        if "target" in mapping and "label" not in mapping:
            warnings.warn(
                "The 'target' key is deprecated! Please use the mapping format in the future.",
                DeprecationWarning,
            )
            mapping["label"] = mapping.pop("target")

        if "label" in mapping and isinstance(mapping["label"], dict):
            warnings.warn(
                "Please use the mapping format for the 'label' key in the future.",
                DeprecationWarning,
            )
            label_dict = mapping["label"]
            label_key = (
                label_dict.get("name")
                or label_dict.get("label")
                or label_dict.get("gold_label")
                or label_dict.get("field")
            )
            if label_key:
                mapping["label"] = label_key
            else:
                raise RuntimeError("Cannot find the 'label' value in the given format!")
            if "metadata_file" in label_dict:
                raise DeprecationWarning(
                    "The 'metadata_file' functionality is deprecated, please modify your source file directly!"
                )
        return mapping

    def _find_reader(self, source_format: str) -> Tuple[Callable, dict]:
        try:
            clean_format = source_format.lower().strip()
            return self.SUPPORTED_FORMATS[clean_format]
        except KeyError:
            raise TypeError(
                f"Format {source_format} not supported. Supported formats are: {', '.join(self.SUPPORTED_FORMATS)}"
            )

    @staticmethod
    def __format_from_source(source: Union[str, List[str]]) -> str:
        if isinstance(source, str):
            source = [source]
        formats = []
        for src in source:
            name, extension = os.path.splitext(src)
            formats.append(extension[1:] if extension else name)

        formats = set(formats)
        if len(formats) != 1:
            raise TypeError(f"source must be homogeneous: {formats}")
        return formats.pop()
