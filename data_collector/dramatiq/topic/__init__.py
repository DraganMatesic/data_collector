"""Declarative queue topology definitions for Dramatiq pipeline routing."""

from data_collector.dramatiq.topic.base import (
    DEAD_LETTERS_QUEUE,
    OCR_TOPIC_EXCHANGE,
    UNROUTABLE_EXCHANGE,
    RegularQueue,
    TopicExchange,
    TopicExchangeQueue,
    get_topic_modules,
)

__all__ = [
    "DEAD_LETTERS_QUEUE",
    "OCR_TOPIC_EXCHANGE",
    "RegularQueue",
    "TopicExchange",
    "TopicExchangeQueue",
    "UNROUTABLE_EXCHANGE",
    "get_topic_modules",
]
