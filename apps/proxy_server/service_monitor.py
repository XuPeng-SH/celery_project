import logging
from kubernetes import client, config, watch
import threading
import queue
import urllib3
import re
from functools import wraps
import time


logger = logging.getLogger(__name__)

incluster_namespace_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

def singleton(cls):
    instances = {}
    @wraps(cls)
    def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return getinstance

class K8SMixin:
    def __init__(self, namespace, in_cluster=False, **kwargs):
        self.namespace = namespace
        self.in_cluster = in_cluster
        self.kwargs = kwargs
        self.v1 = kwargs.get('v1', None)
        if not self.namespace:
            self.namespace = open(incluster_namespace_path).read()

        if not self.v1:
            config.load_incluster_config() if self.in_cluster else config.load_kube_config()
            self.v1 = client.CoreV1Api()


class K8SServiceDiscover(threading.Thread, K8SMixin):
    def __init__(self, message_queue, namespace, in_cluster=False, **kwargs):
        K8SMixin.__init__(self, namespace=namespace, in_cluster=in_cluster, **kwargs)
        threading.Thread.__init__(self)
        self.queue = message_queue
        self.terminate = False

class K8SEventListener(threading.Thread, K8SMixin):
    def __init__(self, message_queue, namespace, in_cluster=False, **kwargs):
        K8SMixin.__init__(self, namespace=namespace, in_cluster=in_cluster, **kwargs)
        threading.Thread.__init__(self)
        self.queue = message_queue
        self.terminate = False

    def run(self):
        resource_version = ''
        start_up = True
        w = watch.Watch()
        for event in w.stream(self.v1.list_namespaced_event, namespace=self.namespace,
                field_selector='involvedObject.kind=Pod'):
            if self.terminate:
                break

            resource_version = int(event['object'].metadata.resource_version)

            info = dict(
                    pod=event['object'].involved_object.name,
                    reason=event['object'].reason,
                    message=event['object'].message,
                    start_up=start_up,
            )
            # logger.info('Received event: {}'.format(info))
            self.queue.put(info)
        '''
        while not self.terminate:
            try:
                w = watch.Watch()
                for event in w.stream(self.v1.list_namespaced_event, namespace=self.namespace,
                        field_selector='involvedObject.kind=Pod', _request_timeout=2, resource_version=resource_version):

                    resource_version = int(event['object'].metadata.resource_version)

                    info = dict(
                            pod=event['object'].involved_object.name,
                            reason=event['object'].reason,
                            message=event['object'].message,
                            start_up=start_up,
                    )
                    # logger.info('Received event: {}'.format(info))
                    self.queue.put(info)
            except urllib3.exceptions.ReadTimeoutError as err:
                start_up = False
                continue
            except ValueError as err:
                time.sleep(1)
                start_up = False
                continue
        '''

class EventHandler(threading.Thread):
    def __init__(self, mgr, message_queue, namespace, pod_patt, **kwargs):
        threading.Thread.__init__(self)
        self.mgr = mgr
        self.queue = message_queue
        self.kwargs = kwargs
        self.terminate = False
        self.pod_patt = re.compile(pod_patt)
        self.namespace = namespace

    def stop(self):
        self.terminate = True

    def on_drop(self, event, **kwargs):
        pass
        # logger.debug('Event Dropped!')

    def on_pod_started(self, event, **kwargs):
        try_cnt = 3
        pod = None
        while  try_cnt > 0:
            try_cnt -= 1
            try:
                pod = self.mgr.v1.read_namespaced_pod(name=event['pod'], namespace=self.namespace)
                if not pod.status.pod_ip:
                    time.sleep(0.5)
                    continue
                break
            except client.rest.ApiException as exc:
                time.sleep(0.5)

        if try_cnt <= 0 and not pod:
            if not event['start_up']:
                logger.error('Pod {} is started but cannot read pod'.format(event['pod']))
            return
        elif try_cnt <= 0 and not pod.status.pod_ip:
            logger.warn('NoPodIPFoundError')
            return

        logger.info('Register POD {} with IP {}'.format(pod.metadata.name, pod.status.pod_ip))
        self.mgr.add_pod(name=pod.metadata.name, ip=pod.status.pod_ip)

    def on_pod_killing(self, event, **kwargs):
        logger.info('Unregister POD {}'.format(event['pod']))
        self.mgr.delete_pod(name=event['pod'])

    def handle_event(self, event):
        if not event or (event['reason'] not in ('Started', 'Killing')):
            return self.on_drop(event)

        if not re.match(self.pod_patt, event['pod']):
            return self.on_drop(event)

        logger.info('Handling event: {}'.format(event))

        if event['reason'] == 'Started':
            return self.on_pod_started(event)

        return self.on_pod_killing(event)

    def run(self):
        while not self.terminate:
            try:
                event = self.queue.get(timeout=1)
                self.handle_event(event)
            except queue.Empty:
                continue

@singleton
class ServiceFounder(object):
    def __init__(self, namespace, pod_patt, in_cluster=False, **kwargs):
        self.namespace = namespace
        self.kwargs = kwargs
        self.queue = queue.Queue()
        self.in_cluster = in_cluster

        self.pod_info = {}

        if not self.namespace:
            self.namespace = open(incluster_namespace_path).read()

        config.load_incluster_config() if self.in_cluster else config.load_kube_config()
        self.v1 = client.CoreV1Api()

        self.listener = K8SEventListener(
                message_queue=self.queue,
                namespace=self.namespace,
                v1=self.v1)

        self.event_handler = EventHandler(mgr=self,
                message_queue=self.queue,
                namespace=self.namespace,
                pod_patt=pod_patt, **kwargs)

    def add_pod(self, name, ip):
        self.pod_info[name] = ip

    def delete_pod(self, name):
        self.pod_info.pop(name, None)

    def start(self):
        self.listener.start()
        self.event_handler.start()

    def stop(self):
        self.listener.stop()
        self.event_handler.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    t = ServiceFounder(namespace='xp', pod_patt=".*-ro-servers-.*", in_cluster=False)
    t.start()
    time.sleep(100)
    print(t.pod_info)
    t.stop()
