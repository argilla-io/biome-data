from biome.data.sources import DataSource
import os

from tests import TESTS_BASEPATH
from tests.test_support import DaskSupportTest

TEST_RESOURCES = os.path.join(TESTS_BASEPATH, "resources")


class CsvDatasourceTest(DaskSupportTest):
    def test_read_csv(self):
        file_path = os.path.join(TEST_RESOURCES, "dataset_source.csv")

        datasource = DataSource(format="csv", path=file_path)
        data_frame = datasource.to_dataframe().compute()

        assert len(data_frame) > 0
        self.assertTrue("path" in data_frame.columns)

    def test_reader_csv_with_leading_and_trailing_spaces_in_examples(self):
        ds = DataSource(
            format="csv",
            source=os.path.join(TEST_RESOURCES, "trailing_coma_in_headers.csv"),
            attributes=dict(sep=";"),
        )
        df = ds.to_dataframe().compute()
        self.assertIn("name", df.columns)
