import logging
import pytest
import random
import numpy as np
from milvus_app import tasks
from milvus_app.factories import (TableFileFactory, TableFactory,
        TableFile, Table, TopKQueryResultFactory)

logger = logging.getLogger(__name__)

def mock_files_topk_results(file_cnt=None):
    file_cnt = file_cnt if file_cnt is not None else random.randint(5, 10)
    files_topk_results = TopKQueryResultFactory.create_batch(file_cnt)
    return files_topk_results

#     ------ nq -------
#     0   1   2   3   4
# | 0 x   x   x   x   x  ==> row 0 is the query result of file_id=0 for all nq
# f 1
# | 2
#
def mock_n_files_top_results(file_cnt=None, nq=None):
    nq = random.randint(5,10) if nq is None else nq
    file_cnt = random.randint(5,10) if file_cnt is None else file_cnt
    files_n_topk_results = []
    for _ in range(file_cnt):
        file_n_topk_results = TopKQueryResultFactory.create_batch(nq)
        files_n_topk_results.append(file_n_topk_results)

    return files_n_topk_results

def test_get_queryable_files(setup_function):
    file_cnt = random.randint(10, 20)
    table = TableFactory()
    TableFileFactory.create_batch(file_cnt, table=table)

    sig = tasks.get_queryable_files.s(table_id=table.table_id)
    results = sig.apply()

    assert len(results.get()) == table.files_to_search().count()

def test_merge_files_query_results(setup_function):
    topK = random.randint(0, 10)
    files_topk_results = mock_files_topk_results()
    sig = tasks.merge_files_query_results.s(files_topk_results, topK)
    results = sig.apply().get()

    flat_results = []
    for file_topk in files_topk_results:
        for each in file_topk.query_results:
            flat_results.append(each)

    expected_results = sorted(flat_results, key=lambda x:x.score)[:topK]
    assert [r.score for r in results.query_results] == [r.score for r in expected_results]

def test_merge_query_results(setup_function):
    topK = random.randint(0, 10)
    files_n_topk_results = mock_n_files_top_results()
    sig = tasks.merge_query_results.s(files_n_topk_results, topK)
    results = sig.apply().get()

    files_n_topk_results
    tranposed = np.asarray(files_n_topk_results).transpose()
    tranposed_list = tranposed.tolist()
    for each_request, test_result in zip(tranposed_list, results):
        flat_results = []
        for file_topk in each_request:
            for each in file_topk.query_results:
                flat_results.append(each)
        expected_results = sorted(flat_results, key=lambda x:x.score)[:topK]
        assert [r.score for r in test_result.query_results] == [r.score for r in expected_results]
