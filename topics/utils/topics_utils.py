from confluent_kafka.admin import NewTopic
from confluent_kafka import KafkaException
import os

def build_topics(cfg, retention_ms):
    """
    Build a list of Kafka NewTopic objects based on the configuration.

    Args:
        cfg (dict): Configuration dictionary containing source information.
        retention_ms (str): Retention period in milliseconds for the topics.

    Returns:
        list: List of NewTopic objects.
    """

    topics = []
    for source in cfg['sources']:
        topic_name = f"hosts.raw.{source['name']}"
        partitions = source.get('topic_partitions', 1)
        replication = source.get('topic_replication', 1)
        topics.append(
            NewTopic(
                topic_name, 
                num_partitions=partitions, 
                replication_factor=replication,
                config={
                    'retention.ms': retention_ms
                }
            )
        )
    return topics

def create_topics(admin, topics, logger):
    """
    Create Kafka topics from built topics list.
    
    Args:
        admin (AdminClient): Kafka AdminClient instance.
        topics (list): List of NewTopic objects to be created.
        logger (logging.Logger): Logger instance for logging.
    
    Returns:
        tuple: Tuple containing:
            - created_topics (int): Number of topics successfully created.
            - existing_topics (int): Number of topics that already existed.
            - failed_topics (int): Number of topics that failed to be created.
    """

    logger.info(f"Topics creation initiated")
    logger.info(f"STATUS: starting | topics_to_create={len(topics)}")
    fs = admin.create_topics(topics)

    created_topics = 0
    existing_topics = 0
    failed_topics = 0
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"STATUS: created | topic={topic}")
            created_topics += 1
        except KafkaException as e:
            if e.args[0].code() == 36:
                logger.info(f"STATUS: already exists | topic={topic}")
                existing_topics += 1
        except Exception as e:
            logger.error(f"STATUS: failed | topic={topic} | error={e}")
            failed_topics += 1

    return created_topics, existing_topics, failed_topics