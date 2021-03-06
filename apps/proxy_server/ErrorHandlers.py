import socket
import sys
import settings
import logging

from milvus.client.Exceptions import *
from MilvusHandler import (ThriftException,)
from proxy_server import server, api

RECONNECT_URI = settings.THRIFTCLIENT_TRANSPORT

LOGGER = logging.getLogger('proxy_server')


@api.err_handler(ThriftException)
def ThriftExceptionHandler(e):
    LOGGER.error(e)
    raise e


@api.err_handler(NotConnectError)
def NotConnectErrorHandler(e):
    LOGGER.error(e)
    api._retry_times += 1
    if api.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(api._retry_times))
        api.reconnect(RECONNECT_URI)
    else:
        server.stop_and_exit()


@api.err_handler(socket.timeout)
def TimeOutHandler(e):
    LOGGER.error(e)
    api._retry_times += 1
    if api.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(api._retry_times))
        api.reconnect(RECONNECT_URI)
    else:
        sys.exit(1)
