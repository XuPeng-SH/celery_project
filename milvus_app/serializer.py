import json
from milvus_app.datatypes import (QueryResponse,)

class JsonCustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, QueryResponse):
            return obj.to_dict()

        return json.JSONEncoder.default(self, obj)

def JsonDecoderHook(obj):
    if '__type__' not in obj:
        return obj

    if obj['__type__'] == QueryResponse.__type__:
        return QueryResponse.from_dict(obj)

def JsonDumps(obj):
    return json.dumps(obj, cls=JsonCustomEncoder)

def JsonLoads(obj):
    return json.loads(obj, object_hook=JsonDecoderHook)

from kombu.serialization import register
register('customjson', JsonDumps, JsonLoads,
        content_type='application/x-customjson',
        content_encoding='utf-8')
