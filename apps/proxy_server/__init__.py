import settings

from milvus_celery.redis_handler import RedisHandler
from proxy_server.service_monitor import ServiceFounder

redis_client = RedisHandler(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
redis_client.init()

from ConnectionHandler import ConnectionHandler
api = ConnectionHandler(uri=settings.THRIFTCLIENT_TRANSPORT)

founder = ServiceFounder(namespace=settings.NAMESPACE,
        pod_patt=settings.ROSERVER_POD_PATT, in_cluster=settings.IN_CLUSTER)

from MilvusHandler import MilvusHandler
handler = MilvusHandler()

from ThriftServer import ThriftServer
server = ThriftServer()

import ErrorHandlers
