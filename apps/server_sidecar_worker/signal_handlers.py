import logging
import os
import socket
import time

from milvus_celery.operations import SDKClient
from celery.signals import (worker_ready, worker_shutdown, worker_init)
from .app import redis_client
from milvus_celery import settings

logger = logging.getLogger(__name__)

servers_monitor_key = os.environ['SERVERS_MONITOR_KEY']
assert servers_monitor_key.startswith('monitor:'), 'Monitor key should start with: "monitor:"'

@worker_ready.connect
def ready_handler(sender=None, **kwargs):
    sdk = SDKClient(host=settings.MILVUS_SERVER_HOST, port=settings.MILVUS_SERVER_PORT)
    ok = False
    time.sleep(3)
    timeout = settings.SIDECAR_WAIT_TIME - 3
    while timeout > 0 and not ok:
        try:
            with sdk:
                status, _ = sdk.client.server_status()
                ok = status.OK()
        except:
            ok = False
        time.sleep(1)
        timeout -= 1

    if not ok:
        logger.error('Server is not ready. Please check it manually')
        return

    logger.info('MonitorKey: {}'.format(servers_monitor_key))
    logger.info('Worker {} is ready'.format(sender.hostname))
    queue = sender.hostname.split('@')[1]
    host = socket.gethostbyname(socket.gethostname())
    redis_client.client.sadd(servers_monitor_key, queue)
    redis_client.client.set(queue, host)

@worker_shutdown.connect
def shutdown_handler(sender=None, **kwargs):
    logger.info('Worker {} is going to shutdown'.format(sender.hostname))
    queue = sender.hostname.split('@')[1]
    redis_client.client.srem(servers_monitor_key, queue)
