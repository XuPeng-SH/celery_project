import sys
sys.path.append('..')
import logging
from __init__ import celery_app
from celery import group, chain, signature

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

celery_app.config_from_object('milvus_app.config')
from milvus_app import tasks


def propagate_chain_get(terminal_node, timeout=None):
    node = terminal_node.parent
    while node:
        node.get(propagate=True, timeout=timeout)
        node = node.parent

def execute_vector_query(table_id, vectors, topK):
    reducer = tasks.merge_query_results.s(topK=topK)
    reducer_result = reducer.freeze()

    r = (
            tasks.get_queryable_files.s(table_id)
            | tasks.schedule_query.s(tasks.query_file.s(vectors, topK), reducer)
        )()

    propagate_chain_get(r)

    return reducer_result

def main():
    results = []
    try:
        for table_id in ['test_group', 'xxxx']:
            async_result = execute_vector_query(table_id, [], 10)
            results.append(async_result)
        for result in results:
            logger.info(result.get(propagate=True, follow_parents=True))
    except Exception as exc:
        logger.exception('')

if __name__ == '__main__':
    main()
