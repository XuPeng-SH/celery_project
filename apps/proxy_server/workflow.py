from query_tasks_worker.tasks import (merge_query_results,
        get_queryable_files, schedule_query, tranpose_n_topk_results,
        reduce_one_request_files_results, reduce_n_request_files_results)

from server_sidecar_worker.tasks import query_files
import settings


def propagate_chain_get(terminal_node, timeout=None):
    node = terminal_node.parent
    while node:
        node.get(propagate=True, timeout=timeout)
        node = node.parent

###########################################################################
##                   1. QueryFile
## QueryFileList ->  2. QueryFile  -> Merge
##                      ...
###########################################################################


def query_vectors_1_n_1_workflow(table_id, vectors, topK, range_array=None):
    queue = settings.QUEUES
    reducer = merge_query_results.s(topK=topK).set(queue=queue)
    reducer_result = reducer.freeze()

    r = (
            get_queryable_files.s(table_id).set(queue=queue)
            | schedule_query.s(query_files.s(vectors, topK).set(queue=queue), reducer).set(queue=queue)
        )()

    propagate_chain_get(r)

    return reducer_result
