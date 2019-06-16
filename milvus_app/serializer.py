import json
from milvus_app.datatypes import (QueryResponse, QueryResultHelper,
        TopKQueryResultHelper, QueryResult, TopKQueryResult)

class JsonCustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, QueryResponse):
            return obj.to_dict()
        if isinstance(obj, QueryResult):
            helper = QueryResultHelper(obj)
            return helper.to_dict()
        if isinstance(obj, TopKQueryResult):
            helper = TopKQueryResultHelper(obj)
            return helper.to_dict()

        return json.JSONEncoder.default(self, obj)

def JsonDecoderHook(obj):
    if '__type__' not in obj:
        return obj

    if obj['__type__'] == QueryResponse.__type__:
        return QueryResponse.from_dict(obj)

    if obj['__type__'] == QueryResultHelper.__type__:
        return QueryResultHelper.from_dict(obj)

    if obj['__type__'] == TopKQueryResultHelper.__type__:
        return TopKQueryResultHelper.from_dict(obj)


def JsonDumps(obj):
    return json.dumps(obj, cls=JsonCustomEncoder)

def JsonLoads(obj):
    return json.loads(obj, object_hook=JsonDecoderHook)

from kombu.serialization import register
register('customjson', JsonDumps, JsonLoads,
        content_type='application/x-customjson',
        content_encoding='utf-8')
