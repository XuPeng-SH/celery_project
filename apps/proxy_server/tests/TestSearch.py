from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
import random
from pprint import pprint


class TestFromServer:
    milvus = Milvus()
    table_name_exists = 'test01'
    table_name_not_exists = 'table' + str(random.random())
    cnn_status = milvus.connect(uri='tcp://localhost:9090')

    # def test_create_table(self):
    #     res0 = self.milvus.create_table(Prepare.table_schema(table_name=self.table_name_not_exists,
    #                                                          dimension=256,
    #                                                          index_type=IndexType.FLAT,
    #                                                          store_raw_vector=False))
    #     assert res0.OK()
    #
    def test_add_vectors(self):
        vectors = Prepare.records([[random.random()for _ in range(256)] for _ in range(20)])
        status, ids = self.milvus.add_vectors(table_name=self.table_name_exists, records=vectors)
        assert status.OK()
        assert isinstance(ids, list)

    def test_search_vectors(self):
        q_records = Prepare.records([[random.random()for _ in range(256)] for _ in range(2)])
        sta, results = self.milvus.search_vectors(table_name=self.table_name_exists,
                                                  query_records=q_records,
                                                  top_k=10)
        pprint(results)
        assert sta.OK()
        assert isinstance(results, (TopKQueryResult, list))
