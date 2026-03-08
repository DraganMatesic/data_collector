"""ORM models for the posts scraper example."""

from sqlalchemy import Column, DateTime, Integer, String, Text, func

from data_collector.examples.scraping import SCHEMA
from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class ExamplePost(Base):
    """Post record from jsonplaceholder.typicode.com."""

    __tablename__ = "example_post"
    __table_args__ = {"schema": SCHEMA}

    id = auto_increment_column()
    post_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    title = Column(String(256), nullable=False)
    body = Column(Text, nullable=False)
    sha = Column(String(64), nullable=False, index=True, comment="Row hash for merge-based sync")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
    date_modified = Column(DateTime, onupdate=func.now())
