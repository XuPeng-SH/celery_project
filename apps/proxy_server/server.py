import os
import sys
import settings
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy_server import server, founder
import MilvusHandler, ConnectionHandler, ErrorHandlers, ThriftServer

def main():
    founder.start()
    server.start_thrift_server()
    founder.stop()


if __name__ == '__main__':
    main()
