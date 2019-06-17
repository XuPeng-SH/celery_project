import sys
sys.path.append('..')
import logging
from __init__ import celery_app
from celery import group, chain, signature
from milvus_app.utils import time_it

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

celery_app.config_from_object('milvus_app.config')
from milvus_app.tasks import (merge_query_results, query_file,
        get_queryable_files, schedule_query, prepare_data_for_final_reduce,
        merge_files_query_results, final_reduce)


def propagate_chain_get(terminal_node, timeout=None):
    node = terminal_node.parent
    while node:
        node.get(propagate=True, timeout=timeout)
        node = node.parent

def execute_vector_query(table_id, vectors, topK):
    reducer = merge_query_results.s(topK=topK)
    reducer_result = reducer.freeze()

    r = (
            get_queryable_files.s(table_id)
            | schedule_query.s(query_file.s(vectors, topK), reducer)
        )()

    propagate_chain_get(r)

    return reducer_result

def vector_search_workflow(table_id, vectors, topK):
    reducer = final_reduce.s()
    reducer_result = reducer.freeze()

    final = (prepare_data_for_final_reduce.s()
            | schedule_query.s(merge_files_query_results.s(topK), reducer))

    r = (
            get_queryable_files.s(table_id)
            | schedule_query.s(query_file.s(vectors, topK), final)
    )()

    propagate_chain_get(r)

    return reducer_result

@time_it
def main():
    results = []
    try:
        for table_id in ['test_group']*1:
            # async_result = vector_search_workflow(table_id, [1]*100, 100)
            async_result = execute_vector_query(table_id, [1]*2000, 100)
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
