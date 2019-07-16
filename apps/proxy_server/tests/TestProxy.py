from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
from milvus.client.Exceptions import ParamError
from milvus.settings import DefaultConfig
from milvus import __version__
import random
import pytest
import time
from pprint import pprint


class TestFromServer:

    @pytest.fixture
    def connect(self, request):
        milvus = Milvus()
        DefaultConfig.THRIFTCLIENT_FRAMED = True
        milvus.connect(uri='tcp://127.0.0.1:9090')

        def fin():
            try:
                milvus.disconnect()
            except Exception:
                pass

        request.addfinalizer(fin)
        return milvus


    @pytest.fixture
    def table(self, request, connect):
        table_name = 'test' + str(random.randint(00000, 99999))
        dim = 256
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_type': IndexType.FLAT,
                 'store_raw_vector': False}
        connect.create_table(param)

        def teardown():
            _, tables = connect.show_tables()
            map(connect.delete_table, tables)

        request.addfinalizer(teardown)
        return table_name

    def test_client_version(self, connect):
        res = connect.client_version()
        assert res == __version__

    def test_connected(self, connect):
        res = connect.connected
        assert res

    def test_server_version(self, connect):
        status, res = connect.server_version()
        assert res == '0.3.1'

    def test_create_table(self, connect):
        table_name_not_exists = 'not_exists'
        res0 = connect.create_table({'table_name': table_name_not_exists,
                                     'dimension': 256,
                                     'index_type': IndexType.FLAT,
                                     'store_raw_vector': False})
        assert res0.OK()

        with pytest.raises(ParamError):
            res1 = connect.create_table({'table_name': table_name_not_exists,
                                         'dimension': 0,
                                         'index_type': IndexType.FLAT})
            assert not res1.OK()

    def test_has_table(self, connect, table):
        res = connect.has_table(table)
        assert res

        res = connect.has_table(table + '_')
        assert not res

    def test_describe_table(self, connect, table):
        res, t = connect.describe_table(table)
        assert res.OK()
        assert t.table_name == table

        res, t = connect.describe_table('table_not_exist')
        assert not res.OK()
        assert not t

    def test_show_tables(self, connect, table):
        status, tables = connect.show_tables()
        assert status.OK()
        assert tables[-1] == table

    def add_vectors(self):
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

    def test_row_counts(self, connect, table):

        sta, result = connect.get_table_row_count(table)
        assert sta.OK()
        assert isinstance(result, int)

        sta, result = connect.get_table_row_count('fake_name')
        assert not sta.OK()
