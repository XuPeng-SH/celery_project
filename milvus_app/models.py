import logging
from sqlalchemy import (Column, Integer, Boolean,
        String, BigInteger, func)
from sqlalchemy.orm import relationship, backref

from milvus_app.db_base import DB

logger = logging.getLogger(__name__)


class TableFile(DB.Model):
    FILE_TYPE_NEW = 0
    FILE_TYPE_RAW = 1
    FILE_TYPE_TO_INDEX = 2
    FILE_TYPE_INDEX = 3
    FILE_TYPE_TO_DELETE = 4

    __tablename__ = 'TableFile'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = Column(String)
    engine_type = Column(Integer)
    file_id = Column(String)
    file_type = Column(Integer)
    size = Column(Integer)
    updated_time = Column(BigInteger)
    created_on = Column(BigInteger)
    date = Column(Integer)

    table = relationship(
        'Table',
        primaryjoin='and_(foreign(TableFile.table_id) == Table.table_id)',
        backref=backref('files', uselist=True, lazy='dynamic')
    )

class Table(DB.Model):

    __tablename__ = 'Table'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = Column(String)
    dimension = Column(Integer)
    engine_type = Column(Integer)
    files_cnt = Column(BigInteger)
    created_on = Column(BigInteger)
    store_raw_data = Column(Boolean)

    def files_to_search(self, date_range=None):
        files = self.files.filter(TableFile.file_type!=TableFile.FILE_TYPE_TO_DELETE)
        return files
