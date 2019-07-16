from milvus import Milvus, Prepare, IndexType
from milvus.client.Abstract import TopKQueryResult
from milvus.client.Exceptions import NotConnectError
from milvus.settings import DefaultConfig
import random
from pprint import pprint
import pytest


class TestConnect:
    milvus = Milvus()
    DefaultConfig.THRIFTCLIENT_FRAMED=True
    cnn_status = milvus.connect(uri='tcp://localhost:9090')

    def test_false_connect1(self):

        _, res = self.milvus.server_version()
        assert res == '0.3.1'
