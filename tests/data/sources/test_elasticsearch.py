import os

from dask.dataframe import DataFrame

from biome.data.sources.readers import from_elasticsearch
from tests.test_support import DaskSupportTest

NPARTITIONS = 3
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "test-index")
ES_DOC = os.getenv("ES_INDEX", "_doc")


class ElasticsearchReaderTest(DaskSupportTest):
    def load_data_to_elasticsearch(self, data, host: str, index: str, doc:str):
        from elasticsearch import Elasticsearch
        from elasticsearch import helpers

        client = Elasticsearch(hosts=host, http_compress=True)
        client.indices.delete(index, ignore_unavailable=True)

        def generator(data):
            for document in data:
                yield {"_index": index, "_type": doc, "_source": document}

        helpers.bulk(client, generator(data))
        del client

    def test_load_data(self):

        self.load_data_to_elasticsearch(
            [dict(a=i, b=f"this is {i}") for i in range(1, 5000)],
            host=ES_HOST,
            index=ES_INDEX,
            doc=ES_DOC
        )

        es_index = from_elasticsearch(
            npartitions=NPARTITIONS, client_kwargs={"hosts": ES_HOST}, index=ES_INDEX, doc_type=ES_DOC
        )

        self.assertTrue(
            isinstance(es_index, DataFrame),
            f"elasticsearch datasource is not a dataframe :{type(es_index)}",
        )
        self.assertTrue(
            es_index.npartitions == NPARTITIONS, "Wrong number of partitions"
        )
        self.assertTrue("id" not in es_index.columns.values, "Expected id as index")
