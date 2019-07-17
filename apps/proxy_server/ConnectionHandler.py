import inspect
import logging
from functools import wraps
from contextlib import contextmanager

from milvus import Milvus
from milvus.thrift.ttypes import (
        TopKQueryResult,
        QueryResult,
        Exception as ThriftException)

import settings

LOGGER = logging.getLogger('proxy_server')


class ConnectionHandler:
    def __init__(self, uri):
        self.uri = uri
        self._retry_times = 0
        self._normal_times = 0
        self.thrift_client = Milvus()
        self.err_handlers = {}
        self.default_error_handler = None

    @contextmanager
    def connect_context(self):
        while self.can_retry:
            try:

                self.thrift_client.connect(uri=self.uri)
                break

            except Exception as e:
                handler = self.err_handlers.get(e.__class__, None)
                if handler:
                    handler(e)
                else:
                    raise e
        yield

        try:

            self.thrift_client.disconnect()
        except Exception:
            self.thrift_client = Milvus()

    def error_collector(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ThriftException) as e:
                handler = self.err_handlers.get(e.__class__, None)
                if handler:
                    handler(e)
                else:
                    raise e
                LOGGER.error(e)
        return inner

    def connect(self, func, handle_error=True):
        @wraps(func)
        def inner(*args, **kwargs):
            with self.connect_context():
                if handle_error:
                    try:
                        return func(*args, **kwargs)
                    except ThriftException as e:
                        handler = self.err_handlers.get(e.__class__, None)
                        if handler:
                            handler(e)
                        else:
                            raise e
                else:
                    return func(*args, **kwargs)

        return inner

    @property
    def client(self):
        return self.thrift_client

    def reconnect(self, uri=None):
        self.uri = uri if uri else self.uri
        self.thrift_client = Milvus()

    @property
    def can_retry(self):
        if self._normal_times >= settings.THRIFTCLIENT_NORMAL_TIME:
            self._retry_times = self._retry_times - 1 if self._retry_times > 0 else 0
            self._normal_times -= settings.THRIFTCLIENT_NORMAL_TIME
        return self._retry_times <= settings.THRIFTCLIENT_RETRY_TIME

    def err_handler(self, exception):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            def wrappers(func):
                self.err_handlers[exception] = func
                return func
            return wrappers
        else:
            self.default_error_handler = exception
            return exception
