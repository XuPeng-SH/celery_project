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

def calculate():
    c = (for tasks.get_data.s() | tasks.do_map.s(-2) | tasks.do_reduce.s())()
    # propagate_chain_get(c)

    logger.error(c.get())


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
    for i in range(1):
        async_result = execute_vector_query('test_group', [], 10)
        results.append(async_result)
    for result in results:
        logger.error(result.failed())
        # logger.error(result.graph)
        # logger.error(result.get(propagate=True, follow_parents=True))

if __name__ == '__main__':
    main()
