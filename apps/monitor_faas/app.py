import sys
sys.path.append('..')
import logging
import fire
from milvus_celery import settings

logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG if settings.DEBUG else logging.INFO)
logger = logging.getLogger(__name__)

class Monitor:
    def __init__(self, application, sink_client, monitor_key):
        self.application = application
        self.sink_client = sink_client
        self.monitor_key = monitor_key

    def on_server_online(self, event):
        logger.info(event)
        hostname = event['hostname']
        msg = '{} is online'.format(event['hostname'])
        logger.info(msg)
        # self.sink_client.client.sadd(self.monitor_key, hostname)

    def on_server_offline(self, event):
        logger.info(event)
        hostname = event['hostname']
        msg = '{} is offline'.format(event['hostname'])
        logger.info(msg)
        self.sink_client.client.srem(self.monitor_key, hostname)

    def run(self):
        with self.application.connection() as connection:
            recv = self.application.events.Receiver(connection,
                    handlers={
                        'worker-online': self.on_server_online,
                        'worker-offline': self.on_server_offline
                    })

            recv.capture(limit=None, timeout=None, wakeup=True)

def main(servers_monitor_key=None):
    import os
    from milvus_celery.app_helper import create_app

    servers_monitor_key = servers_monitor_key if servers_monitor_key else os.environ['SERVERS_MONITOR_KEY']
    assert servers_monitor_key.startswith('monitor:'), 'Monitor key should start with: "monitor:"'

    from milvus_celery.redis_handler import RedisHandler
    redis_client = RedisHandler()
    celery_app = create_app(redis_client=redis_client)

    monitor = Monitor(application=celery_app,
            sink_client=redis_client,
            monitor_key=servers_monitor_key)
    monitor.run()

if __name__ == '__main__':
    fire.Fire(main)
