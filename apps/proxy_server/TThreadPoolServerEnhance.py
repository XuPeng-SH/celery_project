import threading
from six.moves import queue
from thrift.server import TServer
from thrift.transport import TTransport
import logging
import sys

logger = logging.getLogger('proxy_server')


class TThreadPoolServerEnhance(TServer.TThreadPoolServer):
    """Server with a fixed size pool of threads which service requests."""

    def __init__(self, *args, **kwargs):
        TServer.TThreadPoolServer.__init__(self, *args, **kwargs)

    def setNumThreads(self, num):
        """Set the number of worker threads that should be created"""
        self.threads = num

    def serveThread(self):
        """Loop around getting clients from the shared queue and process them."""
        while True:
            try:
                client = self.clients.get()
                self.serveClient(client)
            except Exception as x:
                logger.exception(x)

    def serveClient(self, client):
        """Process input/output from a client for as long as possible"""
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except Exception as x:
            logger.exception(x)
        except SystemExit as e:
            import os
            # TODO
            os._exit(1)

        itrans.close()
        otrans.close()

    def serve(self):
        """Start a fixed number of worker threads and put client into a queue"""
        for i in range(self.threads):
            try:
                t = threading.Thread(target=self.serveThread,daemon=self.daemon)
                # t.setDaemon(self.daemon)
                t.start()
            except Exception as x:
                logger.exception(x)

        # Pump the socket for clients
        self.serverTransport.listen()
        while True:
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                self.clients.put(client)
                # import pdb;pdb.set_trace()
            except Exception as x:
                logger.exception(x)
