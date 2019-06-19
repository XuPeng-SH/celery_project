import logging
import redis

logger = logging.getLogger(__name__)

class RedisHandler:
    def __init__(self, host=None, port=None, password=None, db=None):
        self.host = host if host else 'localhost'
        self.port = port if port else 6379
        self.db = db if db else 0
        self.password = password
        self.client = None

    def init(self, host=None, port=None, password=None, db=None):
        self.host = host if host else self.host
        self.port = port if port else self.port
        self.db = db if db is not None else self.db
        self.password = password if password else self.password

        self.client = redis.Redis(host=self.host,
                port=self.port,
                password=self.password if self.password else None,
                db=self.db)

    def smembers(self, key):
        results = self.client.smembers(key)
        return [result.decode() for result in results]
