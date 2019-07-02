from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
from milvus.client.Exceptions import ParamError
import random
import pytest
from pprint import pprint


class TestFromServer:
    milvus = Milvus()
    table_name_exists = 'test01'
    cnn_status = milvus.connect(uri='tcp://localhost:9090')
    milvus.create_table({'table_name': table_name_exists, 'dimension':256, 'index_type': IndexType.FLAT})

    # if not milvus.has_table(table_name_exists)
    table_name_for_create = 'table' + str(random.random())
    table_name_not_exists = 'table '+ str(random.random())

    def test_client_version(self):
        res = self.milvus.client_version()
        assert res == '0.1.13'

    def test_connected(self):
        res = self.milvus.connected
        assert res

    def test_server_version(self):
        status, res = self.milvus.server_version()
        assert res == '0.3.0'

    def test_create_table(self):
        res0 = self.milvus.create_table(Prepare.table_schema(table_name=self.table_name_for_create,
                                                             dimension=256,
                                                             index_type=IndexType.FLAT,
                                                             store_raw_vector=False))
        assert res0.OK()
        with pytest.raises(ParamError):
            res1 = self.milvus.create_table(Prepare.table_schema(table_name=self.table_name_exists,
                                                             dimension=0,
                                                             index_type=IndexType.FLAT))
            assert not res1.OK()

    def test_has_table(self):
        res = self.milvus.has_table(self.table_name_exists)
        assert res

        res = self.milvus.has_table(self.table_name_not_exists)
        assert not res

    def test_describe_table(self):
        res, table = self.milvus.describe_table(self.table_name_exists)
        assert res.OK()
        assert table.table_name == self.table_name_exists

        res, table = self.milvus.describe_table('table_not_exist')
        assert not res.OK()
        assert not table

    def test_show_tables(self):
        status, tables = self.milvus.show_tables()
        assert status.OK()
        assert isinstance(tables, list)

    def test_add_vectors(self):
        vectors = Prepare.records([[random.random()for _ in range(256)] for _ in range(20)])
        status, ids = self.milvus.add_vectors(table_name=self.table_name_exists, records=vectors)
        assert status.OK()
        assert isinstance(ids, list)

    # def test_search_vectors(self):
    #     q_records = Prepare.records([[random.random()for _ in range(256)] for _ in range(2)])
    #     sta, results = self.milvus.search_vectors(table_name=self.table_name_exists,
    #                                               query_records=q_records,
    #                                               top_k=10)
    #     assert sta.OK()
    #     assert isinstance(results, (TopKQueryResult, list))

    def test_row_counts(self):

        sta, result = self.milvus.get_table_row_count(self.table_name_exists)
        assert sta.OK()
        assert isinstance(result, int)

        sta, result = self.milvus.get_table_row_count('fake_name')
        assert not sta.OK()
