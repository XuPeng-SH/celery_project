import os
import sys
import settings
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy_server import server
import MilvusHandler, ConnectionHandler, ErrorHandlers, ThriftServer


def main():
    settings.config_log()

    server.start_thrift_server()


if __name__ == '__main__':
    main()
