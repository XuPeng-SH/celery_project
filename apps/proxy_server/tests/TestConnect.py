from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
import random
from pprint import pprint
import pytest

class TestConnect:
    milvus = Milvus()
    cnn_status = milvus.connect(uri='tcp://localhost:9091')

    def test_connect(self):
        with pytest.raises(Exception):
            res = self.milvus.show_tables()

    def test_connect1(self):
        with pytest.raises(Exception):
            res = self.milvus.server_version()

    # def test_connect2(self):
    #     with pytest.raises(Exception):
    #         res = self.milvus.create_table('ok')

    def test_connect3(self):
        with pytest.raises(Exception):
            res = self.milvus.delete_table('ok')

    def test_connect4(self):
        with pytest.raises(Exception):
            res = self.milvus.describe_table('ok')


