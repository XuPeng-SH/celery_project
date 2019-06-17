import factory
from factory.alchemy import SQLAlchemyModelFactory
import time
import datetime
import random
from faker import Faker
from faker.providers import BaseProvider
from milvus.client.Abstract import TopKQueryResult, QueryResult

from milvus_app import db
from milvus_app.models import Table, TableFile

class FakerProvider(BaseProvider):
    def random_float(self):
        return (float(Faker().random_number()) * 0.01) % 100

    def query_results(self):
        numbers = random.randint(0, 20)
        results = []
        for _ in range(numbers):
            results.append(QueryResultFactory())
        return results

    def this_date(self):
        t = datetime.datetime.today()
        return (t.year - 1900) * 10000 + (t.month-1)*100 + t.day

factory.Faker.add_provider(FakerProvider)

class QueryResultFactory(factory.Factory):
    class Meta:
        model = QueryResult

    id = factory.Faker('random_number', digits=12, fix_len=True)
    score = factory.Faker('random_float')

class TopKQueryResultFactory(factory.Factory):
    class Meta:
        model = TopKQueryResult

    query_results = factory.Faker('query_results')

class TableFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Table
        sqlalchemy_session = db.Session
        sqlalchemy_session_persistence = 'commit'

    table_id = factory.Faker('uuid4')
    dimension = 512
    engine_type = factory.Faker('random_element', elements=(0,1,2,3))
    files_cnt = 0
    created_on = int(time.time())
    store_raw_data = factory.Faker('random_element', elements=(True, False))

class TableFileFactory(SQLAlchemyModelFactory):
    class Meta:
        model = TableFile
        sqlalchemy_session = db.Session
        sqlalchemy_session_persistence = 'commit'

    table = factory.SubFactory(TableFactory)
    engine_type = factory.Faker('random_element', elements=(0,1,2,3))
    file_id = factory.Faker('uuid4')
    file_type = factory.Faker('random_element', elements=(0,1,2,3,4))
    size = factory.Faker('random_number')
    updated_time = int(time.time())
    created_on = int(time.time())
    date = factory.Faker('this_date')
