import sys
from urllib.parse import urlparse

from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol
from thrift.transport import TTransport, TSocket, TZlibTransport
from thrift.server.TServer import TForkingServer
from thrift.server.TProcessPoolServer import TProcessPoolServer
from thrift.server.TNonblockingServer import TNonblockingServer
from milvus.thrift import MilvusService

from proxy_server import handler
import settings


class Protocol:
    JSON = 'JSON'
    BINARY = 'BINARY'
    COMPACT = 'COMPACT'


class ThriftServer:
    def __init__(self):
        self.server = None
        self.transport = None
        self._pfactory = None
        self._tfactory = None
        self.processor = MilvusService.Processor(handler)

    def set_transport(self):
        _uri = settings.THRIFTSERVER_TRANSPORT
        uri = urlparse(_uri)
        if uri.scheme == "tcp":
            host = uri.hostname
            port = uri.port or 9090
            self.transport = TSocket.TServerSocket(host, port)
        else:
            raise RuntimeError(
                    'Invalid configuration for THRIFTSERVER_TRANSPORT: {}'.format(
                        settings.THRIFTSERVER_TRANSPORT)
                    )

    def set_factory(self):
        if settings.THRIFTSERVER_BUFFERED:
            self._tfactory = TTransport.TBufferedTransportFactory()
        if settings.THRIFTSERVER_ZLIB:
            self._tfactory = TZlibTransport.TZlibTransportFactory()
        if settings.THRIFTSERVER_FRAMED:
            self._tfactory = TTransport.TFramedTransportFactory()

        if settings.THRIFTSERVER_PROTOCOL == Protocol.BINARY:
            self._pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        elif settings.THRIFTSERVER_PROTOCOL == Protocol.COMPACT:
            self._pfactory = TCompactProtocol.TCompactProtocolFactory()
        elif settings.THRIFTSERVER_PROTOCOL == Protocol.JSON:
            self._pfactory = TJSONProtocol.TJSONProtocolFactory()
        else:
            raise RuntimeError(
                    "invalid configuration for THRIFTSERVER_PROTOCOL: {}"
                    .format(settings.THRIFTSERVER_PROTOCOL)
                    )

    def start_thrift_server(self):
        self.set_transport()
        self.set_factory()
        self.server = TForkingServer(self.processor, self.transport, self._tfactory,
                self._pfactory)
        # self.server = TNonblockingServer(self.processor, self.transport)

        self.server.serve()

    def stop_and_exit(self):
        if self.server is not None:
            self.server.stop()
            self.server.close()
            sys.exit(1)
