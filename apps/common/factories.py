import factory
import time
import datetime
import random
from faker import Faker
from faker.providers import BaseProvider
from milvus.client.Abstract import TopKQueryResult, QueryResult

class FakerProvider(BaseProvider):
    def random_float(self):
        return (float(Faker().random_number(digits=8, fix_len=True)) * 0.001) % 100

    def query_results(self):
        numbers = random.randint(0, 20)
        results = []
        for _ in range(numbers):
            results.append(QueryResultFactory())
        return results

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
