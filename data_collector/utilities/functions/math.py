"""functions that perform some kind of calculation"""
from datetime import datetime

def get_totals(start: datetime.now(), end: datetime.now()):
    """Calculates total number of seconds between two datetime"""
    return int((end - start).total_seconds())


def get_totalm(start: datetime.now(), end: datetime.now()):
    """Calculates total number of minutes between two datetime"""
    totalm = int(get_totals(start, end)/60)
    return totalm


def get_totalh(start: datetime.now(), end: datetime.now()):
    """Calculates total number of hours between two datetime"""
    totalh = int(get_totalm(start, end)/60)
    return totalh
