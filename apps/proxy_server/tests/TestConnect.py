from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
from milvus.client.Exceptions import NotConnectError
import random
from pprint import pprint
import pytest


class TestConnect:
    milvus = Milvus()
    cnn_status = milvus.connect(uri='tcp://localhost:9090')

    def test_false_connect1(self):

        _, res = self.milvus.show_tables()


    # def test_connect1(self):
    #     # with pytest.raises(NotConnectError):
    #     _, res = self.milvus.show_tables()
    #     assert _.OK()
    #     assert isinstance(res, list)


    # def test_connect2(self):
    #     with pytest.raises(Exception):
    #         res = self.milvus.server_version()
    # def test_connect3(self):
    #     res = self.milvus.server_version()
    # def test_connect4(self):
    #     res = self.milvus.server_version()
    # def test_connect5(self):
    #     res = self.milvus.server_version()
    # def test_connect6(self):
    #     res = self.milvus.server_version()
    # def test_connect7(self):
    #     res = self.milvus.server_version()
    # def test_connect8(self):
    #     res = self.milvus.server_version()
    # def test_connect9(self):
    #     res = self.milvus.server_version()
    # def test_connect10(self):
    #     res = self.milvus.server_version()

    # def test_false_connect(self):
    #     with pytest.raises(Exception):
    #         res = self.milvus.server_version()


