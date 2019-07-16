import logging
import pytest
import mock
import faker
import random
import sys
sys.path.append('.')
from milvus import Milvus, IndexType, Status
from proxy_server.MilvusHandler import Milvus as Ms
from proxy_server import server


class TestToServer:
    fake_milvus = Milvus()
    fake_milvus.connect(host='127.0.0.1', port='9090')

    @mock.patch.object(Ms, 'server_status')
    def test_ping(self, server_status):
        server_status.return_value = 'OK'
        ans = self.fake_milvus.server_status('fake_ping')
        assert ans == 'OK'

        ans = self.fake_milvus.server_status('version')
        assert ans == 'OK'

    @mock.patch.object(Ms, 'create_table')
    def test_crate_table(self, create_table):
        create_table.return_value = Status.SUCCESS
        ans = self.fake_milvus.create_table('fakeparam')
        assert ans == Status.SUCCESS

    @mock.patch.object(Ms, 'add_vectors')
    def test_add_vector(self, add_vectors):
        add_vectors.return_value = ['aaaa']
        ans = self.fake_milvus.add_vectors('fake1', 'fake2')
        assert ans == ['aaaa']


    @mock.patch.object(Ms, 'describe_table')
    def test_describe_table(self, describe_table):
        describe_table.return_value = 'fake_table_name'
        ans = self.fake_milvus.describe_table('fake_param')
        assert ans == 'fake_table_name'

    @mock.patch.object(Ms, 'show_tables')
    def test_show_tables(self, show_tables):
        show_tables.return_value = 'some_table'
        ans = self.fake_milvus.show_tables()
        assert ans == 'some_table'

    @mock.patch.object(Ms, 'get_table_row_count')
    def test_get_table_row_count(self, get_table_row_count):
        get_table_row_count.return_value = 666
        ans = self.fake_milvus.get_table_row_count('fake_table')
        assert ans == 666



