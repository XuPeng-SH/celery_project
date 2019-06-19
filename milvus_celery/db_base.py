from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

class DB:
    Model = declarative_base()
    def __init__(self, uri=None):
        uri and self.init_db(uri)

    def init_db(self, uri):
        self.engine = create_engine(uri)
        self.uri = uri
        session = sessionmaker()
        session.configure(bind=self.engine)
        self.db_session = session()

    @property
    def Session(self):
        return self.db_session

    def drop_all(self):
        self.Model.metadata.drop_all(self.engine)

    def create_all(self):
        self.Model.metadata.create_all(self.engine)
