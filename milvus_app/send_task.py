import sys
sys.path.append('..')
import logging
from __init__ import celery_app
from celery import group, chain, signature
from milvus_app.utils import time_it

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

celery_app.config_from_object('milvus_app.config')

from milvus_app import workflow

@time_it
def main():
    results = []
    try:
        for table_id in ['test_group']*1:
            #async_result = workflow.query_vectors_1_n_n_1_workflow(table_id, [1]*100, 100)
            async_result = workflow.query_vectors_1_n_1_workflow(table_id, [1]*20, 100)
            results.append(async_result)

        for result in results:
            ret = result.get(propagate=True, follow_parents=True)
            if not ret:
                logger.error('no topk')
                continue
            # for r in ret:
            #     logger.info('-----------------')
            #     for idx, i in enumerate(r.query_results):
            #         logger.info('{} - \t{} {}'.format(idx, i.id, i.score))
    except Exception as exc:
        logger.exception('')

if __name__ == '__main__':
    _, duration = main()
    logger.info('duration={}'.format(duration))
