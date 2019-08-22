import json
import msgpack
import datetime
import logging
from milvus_celery.datatypes import (QueryResponse, QueryResultHelper,
        TopKQueryResultHelper, QueryResult, TopKQueryResult, RowRecord,
        RowRecordHelper, SearchBatchResults, TopKQueryBinResultHelper, TopKQueryBinResult)

logger = logging.getLogger(__name__)

class CustomEncoderMixin:
    def _default(self, obj):
        if isinstance(obj, QueryResponse):
            return obj.to_dict()
        if isinstance(obj, QueryResult):
            helper = QueryResultHelper(obj)
            return helper.to_dict()
        if isinstance(obj, TopKQueryResult):
            helper = TopKQueryResultHelper(obj)
            return helper.to_dict()
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%s')
        if isinstance(obj, RowRecord):
            return RowRecordHelper(obj).to_dict()
        if isinstance(obj, SearchBatchResults):
            return obj.to_dict()
        if isinstance(obj, TopKQueryBinResult):
            return TopKQueryBinResultHelper(obj).to_dict()

        return None;

class JsonCustomEncoder(json.JSONDecoder, CustomEncoderMixin):
    def default(self, obj):
        processed = self._default(obj)
        processed = processed if processed else json.JSONEncoder.default(self, obj)
        return processed

class MsgPackCustomEncoder(CustomEncoderMixin):
    def __call__(self, obj):
        processed = self._default(obj)
        processed = processed if processed else obj
        return processed

def DecoderHook(obj):
    if '__type__' not in obj:
        return obj

    if obj['__type__'] == QueryResponse.__type__:
        return QueryResponse.from_dict(obj)

    if obj['__type__'] == QueryResultHelper.__type__:
        return QueryResultHelper.from_dict(obj)

    if obj['__type__'] == TopKQueryResultHelper.__type__:
        return TopKQueryResultHelper.from_dict(obj)

    if obj['__type__'] == RowRecordHelper.__type__:
        return RowRecordHelper.from_dict(obj)

    if obj['__type__'] == TopKQueryBinResultHelper.__type__:
        return TopKQueryBinResultHelper.from_dict(obj)

    if obj['__type__'] == SearchBatchResults.__type__:
        return SearchBatchResults.from_dict(obj)

def JsonDumps(obj):
    return json.dumps(obj, cls=JsonCustomEncoder)

def JsonLoads(obj):
    if isinstance(obj, bytes):
        obj = obj.decode()
    return json.loads(obj, object_hook=DecoderHook)

msgpack_encoder = MsgPackCustomEncoder()

def MsgPackDumps(obj):
    return msgpack.packb(obj, default=msgpack_encoder, use_bin_type=True)

def MsgPackLoads(obj):
    return msgpack.unpackb(obj, object_hook=DecoderHook, raw=False)

from kombu.serialization import register
register('custommsgpack', MsgPackDumps, MsgPackLoads,
        content_type='application/x-custommsgpack',
        content_encoding='utf-8')
