import os

import pytest
from biome.data.sources import DataSource

from tests import DaskSupportTest, TESTS_BASEPATH

FILES_PATH = os.path.join(TESTS_BASEPATH, "resources")


class DataSourceTest(DaskSupportTest):
    def test_wrong_format(self):

        with pytest.raises(TypeError):
            DataSource(format="not-found")

    def test_add_mock_format(self):
        def ds_parser(**kwargs):
            from dask import dataframe as ddf
            import pandas as pd

            return ddf.from_pandas(
                pd.DataFrame([i for i in range(0, 100)]), npartitions=1
            )

        DataSource.add_supported_format("new-format", ds_parser)
        DataSource.add_supported_format("new-format", ds_parser)
        ds = DataSource(format="new-format")
        self.assertFalse(ds.to_dataframe().columns is None)

    def test_no_mapping_error(self):

        ds = DataSource(
            format="json", path=os.path.join(FILES_PATH, "dataset_source.jsonl")
        )
        with pytest.raises(ValueError):
            ds.to_mapped_dataframe()

    def test_to_mapped(self):
        ds = DataSource(
            format="json",
            mapping={"label": "overall", "tokens": "summary"},
            path=os.path.join(FILES_PATH, "dataset_source.jsonl"),
        )

        df = ds.to_mapped_dataframe()

        self.assertIn("label", df.columns)
        self.assertIn("tokens", df.columns)

        bag = ds.to_mapped_bag().take(1)[0]

        self.assertIn("label", bag)
        self.assertIn("tokens", bag)
