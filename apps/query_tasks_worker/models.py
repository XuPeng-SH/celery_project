import logging
from sqlalchemy import (Integer, Boolean,
        String, BigInteger, func, and_)
from sqlalchemy.orm import relationship, backref

# from milvus_celery.db_base import DB

from . import db

logger = logging.getLogger(__name__)


class TableFile(db.Model):
    FILE_TYPE_NEW = 0
    FILE_TYPE_RAW = 1
    FILE_TYPE_TO_INDEX = 2
    FILE_TYPE_INDEX = 3
    FILE_TYPE_TO_DELETE = 4

    __tablename__ = 'TableFiles'

    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = db.Column(String(50))
    engine_type = db.Column(Integer)
    file_id = db.Column(String(50))
    file_type = db.Column(Integer)
    size = db.Column(Integer, default=0)
    updated_time = db.Column(BigInteger)
    created_on = db.Column(BigInteger)
    date = db.Column(Integer)

    table = relationship(
        'Table',
        primaryjoin='and_(foreign(TableFile.table_id) == Table.table_id)',
        backref=backref('files', uselist=True, lazy='dynamic')
    )

class Table(db.Model):
    TO_DELETE = 1
    NORMAL = 0

    __tablename__ = 'Tables'

    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = db.Column(String(50))
    dimension = db.Column(Integer)
    engine_type = db.Column(Integer)
    files_cnt = db.Column(BigInteger)
    created_on = db.Column(BigInteger)
    store_raw_data = db.Column(Boolean)
    state = db.Column(Integer)

    def files_to_search(self, date_range=None):
        files = self.files.filter(and_(
            TableFile.file_type!=TableFile.FILE_TYPE_TO_DELETE,
            TableFile.file_type!=TableFile.FILE_TYPE_NEW
            ))
        return files
