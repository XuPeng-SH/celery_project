import settings

from ConnectionHandler import ConnectionHandler
api = ConnectionHandler(uri=settings.THRIFTCLIENT_TRANSPORT)

from MilvusHandler import MilvusHandler
handler = MilvusHandler()

from ThriftServer import ThriftServer
server = ThriftServer()

import ErrorHandlers
