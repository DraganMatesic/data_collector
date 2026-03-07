"""ORM models for the books scraper example."""

from sqlalchemy import Column, DateTime, Numeric, String, func

from data_collector.examples.scraping import SCHEMA
from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class ExampleBook(Base):
    """Book record scraped from books.toscrape.com."""

    __tablename__ = "example_book"
    __table_args__ = {"schema": SCHEMA}

    id = auto_increment_column()
    title = Column(String(256), nullable=False)
    price = Column(Numeric(10, 2), nullable=False)
    rating = Column(String(20))
    sha = Column(String(64), nullable=False, index=True, comment="Row hash for merge-based sync")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
    date_modified = Column(DateTime, onupdate=func.now())
