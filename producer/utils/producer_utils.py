import json
from producer.utils.extract_hosts import HostsExtractor

def _process_hosts(hosts, source_info, producer, topic_name, logger, batch_size):
    """
    Produce messages to Kafka topic for the given hosts.
    
    Args:
        hosts (list): List of extracted hosts.
        source_info (dict): Information about the source.
        producer (KafkaProducer): Kafka producer instance.
        topic_name (str): Name of the Kafka topic to produce messages to.
        logger (Logger): Logger instance.
        batch_size (int): Number of messages to send in each batch.
    
    Returns:
        int: Number of failed host messages.
    """

    failed_hosts = 0
    for i, host in enumerate(hosts):
        try:
            message = {
                "name": source_info["name"],
                "url": source_info["url"],
                "category": source_info["category"],
                "retrieval_time": source_info["retrieval_time"],
                "host": host
            }
            producer.produce(topic_name, value=json.dumps(message).encode("utf-8"))
            producer.poll(0)

            if i % batch_size == 0 and i > 0:
                producer.flush()
                logger.info(f"Flushed {i} hosts to topic {topic_name}")
        except Exception as e:
            logger.error(
                f"STATUS: failed | source={source_info['name']} | "
                f"host={host} | error={e}"
            )
            failed_hosts += 1
            continue
    return failed_hosts

def produce_messages(cfg, producer, logger, batch_size=1000):
    """
    Process each source in the configuration, extract hosts, and produce messages to Kafka.

    Args:
        cfg (dict): Configuration dictionary containing sources.
        producer (KafkaProducer): Kafka producer instance.
        logger (Logger): Logger instance.
        batch_size (int): Number of messages to send in each batch.
    
    """

    total_sources = len(cfg['sources'])
    total_hosts_sent = 0
    total_hosts_failed = 0
    failed_sources = 0

    logger.info(f"STATUS: starting | sources_to_process={len(cfg['sources'])}")

    for source_idx, source in enumerate(cfg['sources']):
        try:
            topic_name = f"hosts.raw.{source['name']}"
            source_info = {
                "name": source['name'],
                "url": source.get('url'),
                "type": source.get('type'),
                "category": source.get('category')
            }

            logger.info(
                f"STATUS: processing | source={source['name']} | "
                f"progress={source_idx + 1}/{total_sources}"
            )

            extractor = HostsExtractor(source_info, logger)
            hosts, retrieval_time = extractor.extract_hosts()
            source_info["retrieval_time"] = retrieval_time

            if not hosts:
                logger.error(
                    f"STATUS: failed | source={source['name']} | "
                    f"error=No hosts extracted"
                )
                failed_sources += 1
                continue
            
        except Exception as e:
            logger.error(f"STATUS: failed | source={source['name']} | error={e}")
            failed_sources += 1
            continue

        failed_hosts = _process_hosts(hosts, source_info, producer, 
                                      topic_name, logger, batch_size)

        total_hosts_sent += (len(hosts) - failed_hosts)
        total_hosts_failed += failed_hosts
    
    producer.flush()
    logger.info("Hosts sent to Kafka.")
    return total_hosts_sent, total_hosts_failed, failed_sources