from milvus_app.tasks import (merge_query_results, query_files,
        get_queryable_files, schedule_query, tranpose_n_topk_results,
        reduce_one_request_files_results, reduce_n_request_files_results)

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
def query_vectors_1_n_1_workflow(table_id, vectors, topK):
    reducer = merge_query_results.s(topK=topK)
    reducer_result = reducer.freeze()

    r = (
            get_queryable_files.s(table_id)
            | schedule_query.s(query_files.s(vectors, topK), reducer)
        )()

    propagate_chain_get(r)

    return reducer_result

###########################################################################
## QueryFileList ->  1. QueryFile    1. ReduceEachQuery
##　                 2. QueryFile -> 2. ReduceEachQuery -> Merge
##                      ...            ...
###########################################################################
def query_vectors_1_n_n_1_workflow(table_id, vectors, topK):
    reducer = reduce_n_request_files_results.s()
    reducer_result = reducer.freeze()

    final = (tranpose_n_topk_results.s()
            | schedule_query.s(reduce_one_request_files_results.s(topK), reducer))

    r = (
            get_queryable_files.s(table_id)
            | schedule_query.s(query_files.s(vectors, topK), final)
    )()

    propagate_chain_get(r)

    return reducer_result
