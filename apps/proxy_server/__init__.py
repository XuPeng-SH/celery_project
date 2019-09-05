import settings

from milvus_celery.redis_handler import RedisHandler

redis_client = RedisHandler(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
redis_client.init()

from ConnectionHandler import ConnectionHandler
api = ConnectionHandler(uri=settings.THRIFTCLIENT_TRANSPORT)

from MilvusHandler import MilvusHandler
handler = MilvusHandler()

from ThriftServer import ThriftServer
server = ThriftServer()

import ErrorHandlers
