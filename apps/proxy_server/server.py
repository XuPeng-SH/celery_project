import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol
from thrift.transport import TTransport, TSocket, TZlibTransport
from thrift.server import TServer
from thrift.server.TProcessPoolServer import TProcessPoolServer
from thrift.server.TNonblockingServer import TNonblockingServer
from milvus.thrift import MilvusService

import settings
from MilvusHandler import MilvusHandler
import ErrorHandlers

import sys
if sys.version_info[0] > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


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
        self.processor = MilvusService.Processor(MilvusHandler())
    
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
        self.server = TNonblockingServer(self.processor, self.transport)

        self.server.serve()
        
    def stop_and_exit(self):
        if self.server is not None:
            self.server.stop()
            sys.exit(1)


def run():
    handler = MilvusHandler()
    processor = MilvusService.Processor(handler)

    transport = settings.THRIFTSERVER_TRANSPORT

    config_uri = urlparse(transport)

    _uri = config_uri

    if _uri.scheme == "tcp":

        host = _uri.hostname
        port = _uri.port or 9090

        transport = TSocket.TServerSocket(host, port)
    else:
        raise RuntimeError(
            'Invalid configuration for THRIFTSERVER_TRANSPORT: {transport}'.format(
                transport=settings.THRIFTSERVER_TRANSPORT
            )
        )

    if settings.THRIFTSERVER_BUFFERED:
        tfactory = TTransport.TBufferedTransportFactory()
    if settings.THRIFTSERVER_ZLIB:
        tfactory = TZlibTransport.TZlibTransportFactory()
    if settings.THRIFTSERVER_FRAMED:
        tfactory = TTransport.TFramedTransportFactory()

    if settings.THRIFTSERVER_PROTOCOL == Protocol.BINARY:
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    elif settings.THRIFTSERVER_PROTOCOL == Protocol.COMPACT:
        pfactory = TCompactProtocol.TCompactProtocolFactory()

    elif settings.THRIFTSERVER_PROTOCOL == Protocol.JSON:
        pfactory = TJSONProtocol.TJSONProtocolFactory()

    else:
        raise RuntimeError(
            "invalid configuration for THRIFTSERVER_PROTOCOL: {protocol}"
                .format(protocol=settings.THRIFTSERVER_PROTOCOL)
        )

    # tfactory = TTransport.TFramedTransportFactory()
    # pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # server = TServer.TForkingServer(processor, transport, tfactory, pfactory)
    #server = TProcessPoolServer(processor, transport, tfactory, pfactory)
    server = TNonblockingServer(processor, transport)

    # server.setNumWorkers(12)
    server.serve()


ts = ThriftServer()
ts.start_thrift_server()


#if __name__ == '__main__':
    #run()
    #main()
