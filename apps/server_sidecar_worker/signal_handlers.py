import logging
import os
import socket

from celery.signals import (worker_ready, worker_shutdown, worker_init)
from .app import redis_client

logger = logging.getLogger(__name__)

servers_monitor_key = os.environ['SERVERS_MONITOR_KEY']
assert servers_monitor_key.startswith('monitor:'), 'Monitor key should start with: "monitor:"'

@worker_ready.connect
def ready_handler(sender=None, **kwargs):
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
