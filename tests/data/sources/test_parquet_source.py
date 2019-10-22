import os

from tests import DaskSupportTest, TESTS_BASEPATH

FILES_PATH = os.path.join(TESTS_BASEPATH, "resources")

from biome.data.sources import DataSource


class ParquetDataSourceTest(DaskSupportTest):
    def test_read_parquet(self):
        file_path = os.path.join(FILES_PATH, "test.parquet")
        ds = DataSource(format="parquet", path=file_path)

        df = ds.to_dataframe().compute()
        self.assertTrue("reviewerID" in df.columns)
        self.assertTrue("path" in df.columns)

