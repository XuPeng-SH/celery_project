import os
import logging, logging.config
from environs import Env

env = Env()
env.read_env()

os.environ['ENV_PATH'] = os.path.dirname(os.path.abspath(__file__))
DEBUG = env.bool('DEBUG', default=False)

THRIFTCLIENT_TRANSPORT = env.str('THRIFTCLIENT_TRANSPORT')
THRIFTCLIENT_PROTOCOL = env.str('THRIFTCLIENT_PROTOCOL')
THRIFTCLIENT_BUFFERED = env.bool('THRIFTCLIENT_BUFFERED')
THRIFTCLIENT_ZLIB = env.bool('THRIFTCLIENT_ZLIB')
THRIFTCLIENT_FRAMED = env.bool('THRIFTCLIENT_FRAMED')
THRIFTCLIENT_RETRY_TIME = env.int('THRIFTCLIENT_RETRY_TIME')
THRIFTCLIENT_NORMAL_TIME = env.int('THRIFTCLIENT_NORMAL_TIME')

THRIFTSERVER_TRANSPORT = env.str('THRIFTSERVER_TRANSPORT')
THRIFTSERVER_PROTOCOL = env.str('THRIFTSERVER_PROTOCOL')
THRIFTSERVER_BUFFERED = env.bool('THRIFTSERVER_BUFFERED')
THRIFTSERVER_ZLIB = env.bool('THRIFTSERVER_ZLIB')
THRIFTSERVER_FRAMED = env.bool('THRIFTSERVER_FRAMED')

QUEUES = env.str('QUEUES')

THREAD_POOL_SIZE = env.int('THREAD_POOL_SIZE', 8)

ALL_WORKFLOW = env.bool('ALL_WORKFLOW', False)
REDIS_DB = env.int('REDIS_DB', 0)
REDIS_HOST = env.str('REDIS_HOST', 'localhost')
REDIS_PORT = env.int('REDIS_PORT', 6379)

NAMESPACE = env.str("NAMESPACE", 'default')
ROSERVER_POD_PATT = env.str('ROSERVER_POD_PATT', '.*-ro-servers-.*')
LABEL_SELECTOR = env.str('LABEL_SELECTOR', 'tier=ro-servers')
POLL_INTERVAL = env.int('POLL_INTERVAL', 5)
IN_CLUSTER = env.bool('IN_CLUSTER', True)

LOG_LEVEL = env.str('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')
LOG_PATH = env.str('LOG_PATH', '/tmp/proxy_server')
LOG_NAME = env.str('LOG_NAME', 'proxy_server.log')
TIMEZONE = env.str('TIMEZONE', 'UTC')

from milvus_celery import config_logger
config_logger.config(LOG_LEVEL, LOG_PATH, LOG_NAME, TIMEZONE)
