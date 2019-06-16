
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
