import struct
from milvus.thrift.ttypes import RowRecord
from milvus.client.Abstract import TopKQueryResult, QueryResult
from milvus.thrift.ttypes import TopKQueryBinResult

class QueryResponse:
    __type__ = '__QueryResponse__'

    def __init__(self, result, code=0, msg=''):
        self.result = result
        self.code = code
        self.msg = msg

    def __str__(self):
        return 'QueryResponse:\n\tresult: {}\n\tcode: {}\n\tmsg: {}\n'.format(
                self.result, self.code, self.msg)

    @classmethod
    def BuildErrorResponse(cls, error, result=None):
        return cls(result=result, code=error.code, msg=error.msg)

    def to_dict(self):
        return {
            '__type__': self.__class__.__type__,
            'result': self.result,
            'code': self.code,
            'msg': self.msg
        }

    @classmethod
    def from_dict(cls, d):
        return cls(result=d['result'], code=d['code'], msg=d['msg'])

    @property
    def has_error(self):
        return self.code != 0

class QueryResultHelper:
    __type__ = '__QueryResult__'
    source_class = QueryResult

    def __init__(self, source):
        self.distance = source.distance
        self.id = source.id

    def to_dict(self):
        return {
            '__type__': self.__class__.__type__,
            'distance': self.distance,
            'id': self.id,
        }

    @classmethod
    def from_dict(cls, d):
        return cls.source_class(distance=d['distance'], id=d['id'])


class TopKQueryBinResultHelper:
    __type__ = '__TopKQueryBinResult__'
    source_class = TopKQueryBinResult

    def __init__(self, source):
        self.id_array = source.id_array
        self.distance_array = source.distance_array

    def to_dict(self):
        return {
            '__type__': self.__class__.__type__,
            'id_array': self.id_array,
            'distance_array': self.distance_array
        }

    @classmethod
    def from_dict(cls, d):
        return cls.source_class(id_array=d['id_array'], distance_array=d['distance_array'])


class TopKQueryResultHelper:
    __type__ = '__TopKQueryResult__'
    source_class = TopKQueryResult

    def __init__(self, source):
        self.query_results = source.query_results

    def to_dict(self):
        return {
            '__type__': self.__class__.__type__,
            'query_results': [QueryResultHelper(result).to_dict() for result  in self.query_results]
        }

    @classmethod
    def from_dict(cls, d):
        return cls.source_class(query_results=d['query_results'])

class RowRecordHelper:
    __type__ = '__RowRecordHelper__'
    source_class = RowRecord

    def __init__(self, source):
        self.vector_data = source.vector_data
        self.element_size = struct.calcsize('d')

    def to_dict(self):
        data = {
            '__type__': self.__class__.__type__,
            'data': list(struct.unpack('{}d'.format(int(len(self.vector_data)/self.element_size)), self.vector_data))
        }
        return data

    @classmethod
    def from_dict(cls, d):
        data = struct.pack('{}d'.format(len(d['data'])), *d['data'])
        return RowRecord(data)

class SearchBatchResults:
    __type__ = '__SearchBatchResults__'

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        out = []
        for each_request_topk in self.data:
            id_array, distance_array = [], []
            for each_result in each_request_topk:
                id_array.append(each_result.id)
                distance_array.append(each_result.distance)
            bin_result = TopKQueryBinResult(struct.pack(str(len(id_array))+'l', *id_array),
                    struct.pack(str(len(distance_array))+'d', *distance_array))

            out.append(bin_result)
        data = {
            '__type__': self.__class__.__type__,
            'data': out
        }
        return data

    @classmethod
    def from_dict(cls, d):
        return d['data']

    @classmethod
    def deep_loads(cls, d):
        res = TopKQueryResult()
        for topks in d:
            ids = topks.id_array
            distances = topks.distance_array
            count = len(ids) // 8
            assert count == len(distances) // 8

            ids = struct.unpack(str(count) + 'l', ids)
            distances = struct.unpack(str(count) + 'd', distances)

            qr = [QueryResult(ids[i], distances[i]) for i in range(count)]

            res.append(qr)
        return res
