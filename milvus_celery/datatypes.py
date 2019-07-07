import struct
from milvus.thrift.ttypes import RowRecord
from milvus.client.Abstract import TopKQueryResult, QueryResult

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
