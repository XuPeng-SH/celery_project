import os
ENV_PATH = os.environ.get('ENV_PATH', None)
os.environ['ENV_PATH'] = os.path.dirname(__file__) if ENV_PATH is None else ENV_PATH

import sys
import random
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from milvus_celery.app_helper import create_app
from celery import group, chain, signature
from milvus_celery.utils import time_it
from milvus import Prepare

logger = logging.getLogger('milvus_celery')

from configurations import config
celery_app = create_app(config=config)

from query_workflow_faas import workflow

@time_it
def main():
    results = []
    nq = 3
    topK = 5
    try:
        for table_id in ['test_group']*1:
            dim = 256
            query_records = Prepare.records([[random.random()for _ in range(dim)] for _ in range(nq)])
            async_result = workflow.query_vectors_1_n_1_workflow(table_id, query_records, topK)
            results.append(async_result)

        for result in results:
            ret = result.get(propagate=True, follow_parents=True)
            if not ret:
                logger.error('no topk')
                continue
            for pos, r in enumerate(ret):
                logger.info('-------------Q:={}--------'.format(pos))
                for idx, i in enumerate(r):
                    logger.info('{} - \t{} {}'.format(idx, i.id, i.score))
    except Exception as exc:
        logger.exception('')

if __name__ == '__main__':
    _, duration = main()
    logger.info('duration={}'.format(duration))
