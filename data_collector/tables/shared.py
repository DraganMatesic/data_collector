from sqlalchemy import MetaData

from sqlalchemy.orm import DeclarativeBase
from data_collector.utilities.database import main

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

class Base(DeclarativeBase, main.BaseModel):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)