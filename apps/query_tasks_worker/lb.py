from collections import defaultdict

def simple_router(servers, ids):
    step = len(servers)
    ret = defaultdict(list)
    start = 0
    for i, idx in enumerate(ids):
        ret[servers[i%step]].append(idx)

    return ret

if __name__ == '__main__':
    from collections import defaultdict
    servers = ['milvus-ro-servers-0',
            'milvus-ro-servers-1',
            'milvus-ro-servers-2',
            # 'milvus-ro-servers-3',
            # 'milvus-ro-servers-4',
            # 'milvus-ro-servers-5',
            ]


    keys = ['18', '35', '52', '69', '86', '103', '120', '137', '154', '171', '188', '205', '221', '238', '255', '272', '289', '306', '323', '333', '343', '353', '363', '372', '383', '393', '403', '413', '423', '432', '443', '453', '463', '473', '483', '493', '503', '513', '523', '533', '543', '553', '562', '573', '583', '593', '595']


    mapped = simple_router(servers, keys)

    for k,v in mapped.items():
        print(k, v)
