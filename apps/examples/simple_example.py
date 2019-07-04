# This program demos how to create a Milvus table, insert 20 vectors and get the table row count.

from milvus import Milvus, Prepare, IndexType, Status
import random

milvus = Milvus()

# Connect Milvus server.
# You may need to change HOST and PORT accordingly.
milvus.connect(host='192.168.1.233', port='19530')
# milvus.connect(host='127.0.0.1', port='19531')

# table_name = '500_501'
table_name = '{}_{}'.format(random.randint(800,805), random.randint(800, 802))

# Table name is defined
# table_name = 'yyyyy'
# table_name = 'nnnns'
# table_name = 'test_search_K2YcLOHW'

# Create table: table name, vector dimension and index type
milvus.create_table(dict(table_name=table_name, dimension=256, index_type=IndexType.FLAT))

# Insert 20 256-dim-vectors into demo_table
vectors = [[random.random()for _ in range(256)] for _ in range(200)]
milvus.add_vectors(table_name=table_name, records=vectors)
vectors = [[random.random()for _ in range(256)] for _ in range(10)]

param = {
    'table_name': table_name,
    'query_records': vectors,
    'top_k': 5,
}
status, results = milvus.search_vectors(**param)
print(results)
print(status)

# Get table row count
_, result = milvus.get_table_row_count(table_name=table_name)
print('Table {}, row counts: {}'.format(table_name, result))
