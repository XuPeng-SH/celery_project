class TaskError:
    code = -1
    def __init__(self, msg=''):
        self.msg = self.__class__.__name__
        self.msg = '{}: {}'.format(self.msg, msg) if msg else self.msg

class TableNotFoundError(TaskError):
    code = 10000
