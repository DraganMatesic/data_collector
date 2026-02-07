from sqlalchemy import (
    Column, String, BigInteger,
    DateTime, Integer, text
)

from data_collector.tables.shared import Base
from data_collector.utilities.database.main import auto_increment_column

class CodebookRuntimeCodes(Base):
    __tablename__ = 'c_runtime_codes'

    id = Column(BigInteger, primary_key=True)
    description = Column(String(128))


# Summary of single runtime cycle
class Runtime(Base):
    __tablename__ = 'runtime'
    r"""
    :param runtime: it is datetime.now() 256 hashed value that represents single runtime cycle of an app.
    :param app_id: hashed value of app_group, app_parent, app_name
    :param lsize: size of list that app is using to collect data from source. If None then it is 
        not list base search but crawling based search
    :param start_time: date and time when app started
    :param end_time: date and time when app ended
    :param totals: total runtime in seconds
    :param totalm: total runtime in minutes
    :param totalh: total runtime in hours
    :param except_cnt: total number of exceptions that occurred in runtime
    :param archive: record that is null is last record
    :param date_created: date and time of inserted log record
    """

    id = auto_increment_column()

    runtime = Column(String(length=64), unique=True, index=True)
    app_id = Column(String(length=64), index=True)
    lsize = Column(BigInteger)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    totals = Column(Integer)
    totalm = Column(Integer)
    totalh = Column(Integer)
    except_cnt = Column(Integer, server_default=text("0"))
    exit_code = Column(Integer, server_default=text("0"))
    date_created = Column(DateTime, server_default=text("NOW()"))

    def __eq__(self, other):
        return self.runtime == other.runtime
