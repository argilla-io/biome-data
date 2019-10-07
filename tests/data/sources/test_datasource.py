import os

import pytest
from biome.data.sources import DataSource
from biome.data.sources.datasource import ClassificationForwardConfiguration

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

    def test_forward_transformation_expecting_error(self):

        ds = DataSource(
            format="json", path=os.path.join(FILES_PATH, "dataset_source.jsonl")
        )
        with pytest.raises(ValueError):
            ds.to_forward_dataframe()

    def test_forward_transformation(self):
        ds = DataSource(
            format="json",
            forward=ClassificationForwardConfiguration(
                label="overall", tokens="summary"
            ),
            path=os.path.join(FILES_PATH, "dataset_source.jsonl"),
        )

        data = ds.to_forward_bag().take(1)[0]
        self.assertIn("label", data)
        self.assertIn("tokens", data)

    def test_read_from_yaml_with_metadata_file(self):
        ds = DataSource.from_yaml(os.path.join(FILES_PATH, "datasource.yml"))
        df = ds.to_forward_dataframe()[["tokens", "label"]].compute()

        for v in df["label"].values:
            self.assertIn(
                v, ds.forward.metadata.values(), f"Value {v} should be in metadata"
            )

    def test_read_forward_without_label(self):

        ds = DataSource(
            format="json",
            forward=ClassificationForwardConfiguration(tokens="summary"),
            path=os.path.join(FILES_PATH, "dataset_source.jsonl"),
        )

        ddf = ds.to_forward_dataframe()
        self.assertNotIn("label", ddf.columns)
