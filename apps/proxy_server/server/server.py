import sys
sys.path.append('..')
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol
from thrift.transport import TTransport, TSocket, TZlibTransport
from thrift.server import TServer

from milvus.thrift import MilvusService

from MilvusHandler import MilvusHandler
from settings import DefaultConfig

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
    handler = MilvusHandler()
    processor = MilvusService.Processor(handler)

    transport = DefaultConfig.THRIFTSERVER_TRANSPORT

    config_uri = urlparse(transport)

    _uri = config_uri

    if _uri.scheme == "tcp":

        host = _uri.hostname
        port = _uri.port or 9090

        transport = TSocket.TServerSocket(host, port)
    else:
        raise RuntimeError(
            'Invalid configuration for THRIFTSERVER_TRANSPORT: {transport}'.format(
                transport=DefaultConfig.THRIFTSERVER_TRANSPORT
            )
        )

    if DefaultConfig.THRIFTSERVER_BUFFERED:
        tfactory = TTransport.TBufferedTransportFactory()
    if DefaultConfig.THRIFTSERVER_ZLIB:
        tfactory = TZlibTransport.TZlibTransportFactory()
    if DefaultConfig.THRIFTSERVER_FRAMED:
        tfactory = TTransport.TFramedTransportFactory()

    if DefaultConfig.THRIFTSERVER_PROTOCOL == Protocol.BINARY:
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    elif DefaultConfig.THRIFTSERVER_PROTOCOL == Protocol.COMPACT:
        pfactory = TCompactProtocol.TCompactProtocolFactory()

    elif DefaultConfig.THRIFTSERVER_PROTOCOL == Protocol.JSON:
        pfactory = TJSONProtocol.TJSONProtocolFactory()

    else:
        raise RuntimeError(
            "invalid configuration for THRIFTSERVER_PROTOCOL: {protocol}"
                .format(protocol=DefaultConfig.THRIFTSERVER_PROTOCOL)
        )

    # TODO tfactory default
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()


if __name__ == '__main__':
    run()
