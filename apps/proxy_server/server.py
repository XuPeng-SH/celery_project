import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol
from thrift.transport import TTransport, TSocket, TZlibTransport
from thrift.server import TServer

from milvus.thrift import MilvusService

import settings
from MilvusHandler import MilvusHandler

import sys
if sys.version_info[0] > 2:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class Protocol:
    JSON = 'JSON'
    BINARY = 'BINARY'
    COMPACT = 'COMPACT'


def run():
    # TODO how to close
    handler = MilvusHandler(uri=settings.THRIFTCLIENT_TRANSPORT)
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
                .format(protocol=settingss.THRIFTSERVER_PROTOCOL)
        )

    # TODO tfactory default
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()


if __name__ == '__main__':
    run()
