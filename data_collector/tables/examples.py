from sqlalchemy import (
    Column, String,
    DateTime, Integer, Date, func
)


from data_collector.tables.shared import Base
from data_collector.utilities.database.main import auto_increment_column

class ExampleTable(Base):
    """
    Used for examples described in documentation
    """
    __tablename__ = 'example_table'

    # Auto-incrementing ID
    id = auto_increment_column()
    company_id = Column(Integer, index=True)
    person_id = Column(Integer, index=True)
    name = Column(String(15))
    surname = Column(String(25))
    birth_date = Column(Date)
    sha = Column(String(64))
    archive = Column(DateTime, comment="data and time this database object was removed from usage")
    date_created = Column(DateTime, server_default=func.now())  # DateCreated
