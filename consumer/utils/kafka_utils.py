import os
import re
import time
from confluent_kafka import Consumer

def create_costumer(logger, retries=3, wait_time=5):
    """Create and return a Kafka consumer subscribed to relevant topics.
    Args:
        logger (Logger): Logger instance for logging information and errors.
    Returns:
        Consumer: Configured Kafka consumer instance.
    """
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id": os.getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })
    for attempt in range(retries):
        try:
            metadata = consumer.list_topics(timeout=5)
            topics = list(metadata.topics.keys())
            matched_topics = [topic for topic in topics if re.match("^hosts\\.raw\\..*", topic)]
            consumer.subscribe(matched_topics)
            logger.info(f"Subscribed to topics: {matched_topics}")
            return consumer
        except Exception as e:
            logger.error(
                f"Error connecting to Kafka: {e}"
                f"Attempt {attempt + 1} of {retries}. Retrying in {wait_time} seconds."
            , exc_info=True)
            time.sleep(wait_time)
    logger.error("Exceeded maximum retries. Could not connect to Kafka.")
    raise Exception("Could not connect to Kafka after retries.")