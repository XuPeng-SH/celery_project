from environs import Env
from db_base import DB
from models import TableFile, Table

env = Env()
env.read_env()
DB_URI = env.str('DB_URI')

db = DB(uri=DB_URI)

def show_file(f):
    print(f.id)
    print(f.table_id)
    print(f.date)
    print(f.created_on)

tables = db.Session.query(Table).all()
for t in tables:
    print('table {}'.format(t.table_id))
    files = t.files.filter_by(file_type=TableFile.FILE_TYPE_INDEX)
    for f in files:
        show_file(f)
