import socket
import sys
import settings
import logging

from MilvusHandler import connect
from milvus.client.Exceptions import *
from MilvusHandler import (ThriftException,)


RECONNECT_URI = settings.THRIFTCLIENT_TRANSPORT

LOGGER = logging.getLogger('proxy_server')


@connect.err_handler(ThriftException)
def ThriftExceptionHandler(e):
    LOGGER.error(e)
    raise e


@connect.err_handler(NotConnectError)
def NotConnectErrorHandler(e):
    LOGGER.error(e)
    connect._retry_times += 1
    if connect.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(connect._retry_times))
        connect.reconnect(RECONNECT_URI)
    else:
        sys.exit(1)


@connect.err_handler(socket.timeout)
def TimeOutHandler(e):
    LOGGER.error(e)
    connect._retry_times += 1
    if connect.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(connect._retry_times))
        connect.reconnect(RECONNECT_URI)
    else:
        sys.exit(1)
