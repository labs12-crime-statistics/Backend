from sqlalchemy import create_engine, inspect
from decouple import config
from models import *


DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
BASE.metadata.drop_all(bind=ENGINE)
BASE.metadata.create_all(bind=ENGINE)

ins = inspect(ENGINE)
for _t in ins.get_table_names():
    print(_t)
