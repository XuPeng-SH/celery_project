import logging
from celery.signals import task_postrun

logger = logging.getLogger(__name__)

@task_postrun.connect
def task_postrun_handler(sender=None,
        task_id=None,
        task=None,
        args=None,
        kwargs=None,
        retval=None,
        state=None,
        **kwds):
    logger.debug('sender: {}\ntask_id: {}\nargs: {}\nkwargs: {}'.format(sender,
        task_id, args, kwargs))
